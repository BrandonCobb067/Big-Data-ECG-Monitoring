# Allow both python src/replay.py and python -m src.replay from the project root.
import sys
from pathlib import Path

if __package__ in (None, ''):
    sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

"""Concurrent, paced patient replay with acknowledgement after durable storage."""
import argparse
import csv
import json
import socket
import time
from concurrent.futures import ThreadPoolExecutor
from contextlib import closing
import numpy as np
from src.signal_processing import decode, make_filter
from src.readers.chapman_reader import ChapmanReader
from src.readers.mitbih_reader import MitBihReader
from src.models.ecg_record import ECGRecord
from src.transport import EdgeEncoder, send_message, receive_message


def load_records(dataset, input_path, patients, windows):
    if dataset == 'chapman':
        if not input_path.is_file():
            raise ValueError('Chapman needs an ECGData.zip file: ' + str(input_path))
        if windows > 5:
            raise ValueError('Chapman recordings allow at most five two-second windows')
        reader = ChapmanReader(input_path)
    else:
        if not input_path.is_dir() or not any(input_path.rglob('*.hea')):
            raise ValueError('Extract MIT-BIH .hea/.dat files into ' + str(input_path) + ' or select their folder with --input')
        reader = MitBihReader(input_path)

    records = []
    completed = set()
    current = None
    # Join chunks from the same recording, so they are not counted as different patients.
    with closing(reader.yield_chunks()) as chunks:
        for chunk in chunks:
            source = chunk.record_id
            start = 0
            if dataset == 'mitbih':
                source, offset = chunk.record_id.rsplit('_', 1)
                start = int(offset)
            if source in completed:
                continue
            if current is None:
                current = ECGRecord(source, chunk.sample_rate, chunk.leads.copy(), chunk.units, [])
            if source != current.record_id:
                raise ValueError('Requested duration exceeds recording ' + current.record_id)
            if start != len(current.samples):
                raise ValueError('Gap or overlap in recording ' + source)
            if (chunk.sample_rate, chunk.leads, chunk.units) != (current.sample_rate, current.leads, 'uV'):
                raise ValueError('Unexpected sample metadata in ' + source)
            needed = windows * 2 * current.sample_rate
            current.samples.extend(chunk.samples[:needed - len(current.samples)])
            if len(current.samples) == needed:
                current.samples = np.asarray(current.samples, dtype=float)
                if current.samples.shape != (needed, len(current.leads)) or not np.isfinite(current.samples).all():
                    raise ValueError('Invalid or missing samples in ' + source + '; clean the source before replay')
                records.append(current)
                completed.add(source)
                current = None
                if len(records) == patients:
                    break
    if len(records) != patients:
        raise ValueError('Not enough complete source recordings for the requested patients/windows')
    return records


def replay_patient(index, record, args, beginning):
    record_id = record.record_id
    samples = np.asarray(record.samples)
    rate = record.sample_rate
    encoder = EdgeEncoder(rate,samples.shape[1],args.mode)
    packets = []
    rows = []
    with socket.create_connection((args.host,args.port),timeout=300) as connection:
        connection.settimeout(300)
        for window in range(args.windows):
            start = window*2*rate
            values = samples[start:start+2*rate]
            if not len(values):
                break
            deadline = beginning+(start+len(values))/rate/args.rate
            time.sleep(max(0,deadline-time.perf_counter()))
            started_unix = time.time()
            started = time.perf_counter()
            packet, aggregates = encoder.encode(values,start)
            edge_seconds = time.perf_counter()-started
            identity = args.run+':'+str(index)+':'+str(start)
            document = {'run':args.run,'id':identity,'dataset':args.dataset,'mode':args.mode,
                        'stream_id':index,'source_record':record_id,'original_rate':rate,
                        'start_sample':start,'original_rows':len(values),'units':'uV',
                        'packet':packet,'edge_aggregates':aggregates}
            sent = send_message(connection,document)
            response, received = receive_message(connection)
            finished = time.perf_counter()
            if not response.get('ok') or response.get('id')!=identity:
                raise RuntimeError(str(response))
            rows.append({'stream':index,'record':record_id,'window':window,'sent_bytes':sent,
                         'started_unix':started_unix,'acknowledged_unix':time.time(),
                         'ack_bytes':received,'encode_seconds':edge_seconds,
                         'end_to_end_seconds':finished-started,'schedule_lag_seconds':max(0,started-deadline),
                         'scheduled_to_ack_seconds':finished-deadline, **{key:value for key,value in response.items() if key not in ('id','ok')}})
            packets.append(packet)
    used = samples[:len(packets)*2*rate]
    restored = decode(packets,make_filter(rate))
    margin = 128
    reference = used[margin:len(used)-margin]
    if args.mode=='edge':
        restored = restored[2*margin:len(used)]
    else:
        np.testing.assert_array_equal(restored,used)
        restored = restored[margin:len(used)-margin]
    error = restored-reference
    quality = {'stream':index,'record':record_id,'squared_error':float(np.sum(error*error)),
               'scored_values':reference.size,'max_error_uv':float(np.max(np.abs(error))),
               'diagnostic_accuracy':None,'diagnostic_status':'not evaluated; no labeled classifier configured'}
    return rows,quality,finished


def main():
    parser=argparse.ArgumentParser(description='Replay data through the existing Chapman and MIT-BIH readers. Requires a compatible TCP receiver.')
    parser.add_argument('--run',required=True)
    parser.add_argument('--dataset',choices=['chapman','mitbih'],required=True)
    parser.add_argument('--input',type=Path,help='Chapman ZIP or extracted MIT-BIH folder (defaults to data/<dataset>/raw)')
    parser.add_argument('--mode',choices=['raw','edge'],required=True)
    parser.add_argument('--patients',type=int,default=2)
    parser.add_argument('--windows',type=int,default=5)
    parser.add_argument('--rate',type=float,default=1)
    parser.add_argument('--host',default='127.0.0.1')
    parser.add_argument('--port',type=int,default=8765)
    parser.add_argument('--output',type=Path,required=True)
    args=parser.parse_args()
    if not 1<=args.patients<=128 or args.windows<1 or not np.isfinite(args.rate) or args.rate<=0:
        parser.error('Invalid load configuration (patients 1..128, positive windows/rate)')
    if args.input is None:
        args.input=Path('data')/args.dataset/'raw'
        if args.dataset=='chapman':
            args.input=args.input/'ECGData.zip'
    records=load_records(args.dataset,args.input,args.patients,args.windows)
    args.output.mkdir(parents=True,exist_ok=False)
    beginning=time.perf_counter()
    all_rows=[]
    quality=[]
    last_ack=beginning
    with ThreadPoolExecutor(max_workers=args.patients) as pool:
        futures=[]
        for index,record in enumerate(records):
            futures.append(pool.submit(replay_patient,index,record,args,beginning))
        for future in futures:
            rows,result,finished=future.result()
            all_rows.extend(rows)
            quality.append(result)
            last_ack=max(last_ack,finished)
    # Offline waveform scoring must not inflate the measured transport duration.
    elapsed=last_ack-beginning
    with (args.output/'windows.csv').open('w',newline='') as output:
        writer=csv.DictWriter(output,fieldnames=list(all_rows[0]))
        writer.writeheader()
        writer.writerows(all_rows)
    latency=np.array([row['end_to_end_seconds'] for row in all_rows])
    summary={'run':args.run,'dataset':args.dataset,'mode':args.mode,'patients':args.patients,
             'replay_rate':args.rate,'windows':len(all_rows),'elapsed_seconds':elapsed,
             'sent_bytes':sum(row['sent_bytes'] for row in all_rows),
             'ack_bytes':sum(row['ack_bytes'] for row in all_rows),
             'latency_p50_seconds':float(np.median(latency)),'latency_p95_seconds':float(np.percentile(latency,95)),
             'schedule_lag_max_seconds':max(row['schedule_lag_seconds'] for row in all_rows),
             'rmse_uv':float(np.sqrt(sum(row['squared_error'] for row in quality)/sum(row['scored_values'] for row in quality))),
             'max_error_uv':max(row['max_error_uv'] for row in quality),'diagnostic_accuracy':None,
             'diagnostic_status':'not evaluated', 'complete':True}
    summary['application_bytes_per_second']=summary['sent_bytes']/elapsed
    (args.output/'summary.json').write_text(json.dumps(summary,indent=2))
    (args.output/'quality.json').write_text(json.dumps(quality,indent=2))
    print(json.dumps(summary,indent=2))


if __name__=='__main__':
    main()
