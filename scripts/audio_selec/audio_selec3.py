import os
import io
import logging
import soundfile as sf
import numpy as np
from minio import Minio
from io import BytesIO
from urllib.parse import urlparse
from datetime import datetime, timedelta
from flask import Flask, request, jsonify

app = Flask(__name__)

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

global_wavinfo_list = []

def datetime_to_str(dt):
    return f"{dt:%Y-%m-%d %H:%M:%S}.{dt.microsecond//1000:03d}"

def str_to_datetime(str):
    return datetime.strptime(str, "%Y-%m-%d %H:%M:%S.%f")

def download_wav_from_minio(minio_url):
    logging.info(f"download {minio_url}")
    parsed_url = urlparse(minio_url)
    minio_url = f"{parsed_url.scheme}://{parsed_url.hostname}:{parsed_url.port}"
    path_parts = parsed_url.path.strip("/").split("/")
    bucket_name = path_parts[0]
    file_path = "/".join(path_parts[1:])

    client = Minio(
        parsed_url.hostname + (f":{parsed_url.port}" if parsed_url.port else ""),
        access_key = "minioadmin",
        secret_key = "minioadmin",
        secure = parsed_url.scheme == "https"
    )

    try:
        response = client.get_object(bucket_name, file_path)
        bytes_io = BytesIO(response.read())
        response.close()
        response.release_conn()

        bytes_io.seek(0)
        data, sample_rate = sf.read(bytes_io)
        bytes_io.seek(0)
        # print(f'data:{data}')
        # print(f'sample_rate:{sample_rate}')
        return data, sample_rate

    except Exception as e:
        logging.error(f"Error: Failed to retrieve WAV stream from Minio: {e}")
        return None, None
    
def upload_wav_to_minio(wav_data, sample_rate, file_url):
    if wav_data is None:
        return {"error": "No wav data to upload"}

    parsed_url = urlparse(file_url)
    minio_url = f"{parsed_url.scheme}://{parsed_url.hostname}:{parsed_url.port}"
    path_parts = parsed_url.path.strip("/").split("/")
    bucket_name = path_parts[0]
    file_path = "/".join(path_parts[1:])

    client = Minio(
        parsed_url.hostname + (f":{parsed_url.port}" if parsed_url.port else ""),
        access_key = "minioadmin",
        secret_key = "minioadmin",
        secure = parsed_url.scheme == "https"
    )

    try:
        wav_stream = BytesIO()
        sf.write(wav_stream, wav_data, sample_rate, format="WAV")
        wav_stream.seek(0)
        audio_size = wav_stream.getbuffer().nbytes

        client.put_object(
            bucket_name = bucket_name,
            object_name = file_path,
            data = wav_stream,
            length = audio_size,
            content_type = "audio/wav"
        )
        logging.info(f"File successfully uploaded to {file_url}")
        return {"message": "File uploaded successfully", "filePath": file_url}

    except Exception as e:
        logging.error(f"Error occurred during upload: {e}")
        return {"error": str(e)}

def calc_score(file_paths):
    score = len(file_paths)
    return score

def calc_wav_info(wav_data, sample_rate):
    try:
        duration = int(len(wav_data) * 1000 / sample_rate)
        wav_size = wav_data.nbytes

        return wav_size, duration

    except Exception as e:
        logging.error(f"Error: Failed to calculate WAV info: {e}")
        return None, None

def calc_wav_endtime(begin_time, duration):
    return datetime_to_str(begin_time + timedelta(milliseconds = duration))

def extract_from_wav_data(wav_data, sample_rate, start_ms, end_ms):
    try:
        start_sample = int(start_ms * sample_rate / 1000)
        end_sample = int(end_ms * sample_rate / 1000)
        end_sample = min(end_sample, len(wav_data))

        segment = wav_data[start_sample:end_sample]

        return segment, sample_rate

    except Exception as e:
        logging.error(f"Error: Failed to process WAV stream: {e}")
        return None, None

def splice_other_wav(other_machine, begin, end):
    idx = 0
    while idx+1 < len(other_machine) and other_machine[idx]["beginTime"] < begin:
        print(f'idx:{idx}  other_machine[{idx}]["beginTime"]:{other_machine[idx]["beginTime"]}  other_machine[{idx}]["endTime"]:{other_machine[idx]["endTime"]}  begin:{begin}')
        if other_machine[idx]["beginTime"] < begin and other_machine[idx]["endTime"] > begin:
            break
        idx += 1

    elements = [other_machine[idx]]
    front_end_time = other_machine[idx]["endTime"]
    front_exceed_time = 0
    front_idx = idx
    while idx < len(other_machine) and other_machine[idx]["beginTime"] <= end:
        if other_machine[idx]["beginTime"] < front_end_time:
            current_exceed_time = (str_to_datetime(other_machine[idx]["endTime"]) - str_to_datetime(front_end_time)).total_seconds() * 1000
            print(f'current_exceed_time:{current_exceed_time}')
            print(f'front_exceed_time:{front_exceed_time}')
            print(f'front_end_time:{front_end_time}')
            if current_exceed_time > front_exceed_time:
                front_exceed_time = current_exceed_time
                front_idx = idx
        else:
            elements.append(other_machine[front_idx])
            front_end_time = other_machine[front_idx]["endTime"]
            front_exceed_time = 0
            front_idx = idx
        if idx == len(other_machine) - 1:
            break
        else:
            idx += 1
    elements.append(other_machine[front_idx])

    wav_data_list = []
    end_time_tmp = begin
    trim_tmp = 0
    begin_sound = 0
    for item in elements:
        file_path = item["filePath"]
        logging.info(f"file_path:{file_path}")
        wav_data_t, sample_rate = download_wav_from_minio(file_path)

        start_trim = (str_to_datetime(end_time_tmp) - str_to_datetime(item["beginTime"])).total_seconds() * 1000
        end_trim = min((str_to_datetime(end) - str_to_datetime(item["beginTime"])).total_seconds() * 1000, item["duration"])
        if start_trim < 0:
            logging.info(f"sil before segment:{-1*start_trim:.0f}ms")
            trim_tmp += start_trim
            start_trim = 0
        if start_trim >= end_trim:
            continue
        print(f'start_trim:{start_trim}  end_trim:{end_trim}')
        wav_data_list.append(extract_from_wav_data(wav_data_t, sample_rate, start_trim, end_trim))

        end_time_tmp = item["endTime"]
        logging.info(f"all sil before segment:{-1*trim_tmp:.0f}ms\n")

        # micro_second = 0
        # for wavinfo in global_wavinfo_list:
        #     micro_second += wavinfo["duration"]
        global_wavinfo_list.append({
            "sourceUuid": item["sourceUuid"],
            "filePath": item["filePath"],
            "sampleMachine": item["sampleMachine"],
            "inputBeginTime": calc_wav_endtime(str_to_datetime(item["beginTime"]), start_trim),
            "inputEndTime": calc_wav_endtime(str_to_datetime(item["beginTime"]), end_trim),
            "beginMilliSecond": int(sum(wavinfo["duration"] for wavinfo in global_wavinfo_list)),
            "duration": int(end_trim - start_trim)
        })

    print(f'wav_data_list:{wav_data_list}')
    for item in wav_data_list:
        print(f'data:{item[0]}')
        print(f'sample_rate:{item[1]}')

    return wav_data_list

@app.route("/process_audio3", methods=["POST"])
def process_wav():
    input_json = request.json
    if not input_json:
        return jsonify({"error": "Invalid input, JSON data required"}), 400

    dic_sample_machine = {}
    for item in input_json["list"]:
        sample_machine = item["sampleMachine"]
        if sample_machine not in dic_sample_machine:
            dic_sample_machine[sample_machine] = []
        dic_sample_machine[sample_machine].append(item)

    dic_wav_score = {}
    for key, value in dic_sample_machine.items():
        file_paths = [item['filePath'] for item in value]
        dic_wav_score[key] = calc_score(file_paths)
    print(f'dic_wav_score:{dic_wav_score}')

    max_key = max(dic_wav_score, key=dic_wav_score.get)
    max_value = dic_wav_score[max_key]
    print(f'max_key:{max_key}')
    print(f'max_value:{max_value}')

    best_machine = []
    other_machine = []
    for item in input_json["list"]:
        # print(f'item["filePath"]:{item["filePath"]}')
        wav_data, sample_rate = download_wav_from_minio(item["filePath"])
        _, item["duration"] = calc_wav_info(wav_data, sample_rate)
        item["endTime"] = calc_wav_endtime(str_to_datetime(item["beginTime"]), item["duration"])
        if item["sampleMachine"] == max_key:
            best_machine.append(item)
        else:
            other_machine.append(item)
    best_machine.sort(key=lambda x: str_to_datetime(x["beginTime"]))
    other_machine.sort(key=lambda x: str_to_datetime(x["beginTime"]))
    print(f'best_machine:')
    for item in best_machine:
        print(item)
    print(f'len(best_machine):{len(best_machine)}')

    wav_data_list = []
    idx = 0
    while idx <= len(best_machine):
        if idx == 0:
            begin_time = input_json["begin"]
            end_time = best_machine[idx]["beginTime"]
        elif idx == len(best_machine):
            begin_time = best_machine[idx-1]["endTime"]
            end_time = input_json["end"]
        else:
            begin_time = best_machine[idx-1]["endTime"]
            end_time = best_machine[idx]["beginTime"]

        if other_machine:
            print(f'begin_time:{begin_time}')
            print(f'end_time:{end_time}')
            wav_data_list.extend(splice_other_wav(other_machine, begin_time, end_time))
        
        if idx < len(best_machine):
            wav_data_list.append(download_wav_from_minio(best_machine[idx]["filePath"]))

            global_wavinfo_list.append({
                "sourceUuid": best_machine[idx]["sourceUuid"],
                "filePath": best_machine[idx]["filePath"],
                "sampleMachine": best_machine[idx]["sampleMachine"],
                "inputBeginTime": best_machine[idx]["beginTime"],
                "inputEndTime": best_machine[idx]["endTime"],
                "beginMilliSecond": int(sum(wavinfo["duration"] for wavinfo in global_wavinfo_list)),
                "duration": int((str_to_datetime(best_machine[idx]["endTime"]) - str_to_datetime(best_machine[idx]["beginTime"])).total_seconds() * 1000)
            })
        idx += 1

    res_path = f"http://{input_json['uploadHost']}/{input_json['uploadBucket']}/{input_json['uploadObject']}"
    wav_data = np.concatenate([sublist[0] for sublist in wav_data_list if sublist[0].ndim > 0], axis=0)
    sample_rates = [sublist[1] for sublist in wav_data_list]
    print(f'sample_rates:{sample_rates}')
    sample_rate = sample_rates[0] if len(set(sample_rates)) == 1 else None
    print(f'sample_rate:{sample_rate}')
    if sample_rate is None:
        return jsonify({"error": "sample rate is different"}), 400
    
    upload_res = upload_wav_to_minio(wav_data, sample_rate, res_path)
    if "error" in upload_res:
        return jsonify(upload_res), 500

    file_size, duration = calc_wav_info(wav_data, sample_rate)
    output_json = {
        "begin": input_json["begin"],
        "end": input_json["end"],
        "fileDuration": duration,
        "fileSize": file_size,
        "filePath": res_path,
        "list": global_wavinfo_list
    }

    return jsonify(output_json), 200

if __name__ == "__main__":
    app.run(host='0.0.0.0', port=5003, debug=True)
