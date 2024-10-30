# 使用mino，没有使用http
from pydub import AudioSegment
import json
from minio import Minio
from io import BytesIO
from datetime import datetime, timedelta
from urllib.parse import urlparse

def read_json(jsonPath):
    with open(jsonPath, "r") as f:
        input_json = json.load(f)

    input_json["list"].sort(key=lambda x: datetime.strptime(x["beginTime"], "%Y-%m-%d %H:%M:%S.%f"))
    return input_json

def download_audio_from_minio(minio_url):
    parsed_url = urlparse(minio_url)
    path_parts = parsed_url.path.lstrip("/").split("/", 1)
    if len(path_parts) < 1:
        print("Error: No bucket name found in the URL.")
        return None, 0

    host = f"{parsed_url.hostname}:{parsed_url.port}"
    bucket_name = path_parts[0]
    object_name = path_parts[1] if len(path_parts) > 1 else ""

    minio_client = Minio(
        host,
        access_key = "minioadmin",
        secret_key = "minioadmin",
        secure=False
    )
    
    try:
        response = minio_client.get_object(bucket_name, object_name)
        audio_input_json = BytesIO(response.read())
        response.close()
        response.release_conn()
        audio = AudioSegment.from_wav(audio_input_json)
        return audio, len(audio)
    
    except Exception as e:
        print("Error:", e)
        return None, 0

def format_time(dt):
    return dt.strftime('%Y-%m-%d %H:%M:%S.%f')

def audio_selec(input_json):
    begin_time = datetime.strptime(input_json["begin"], "%Y-%m-%d %H:%M:%S.%f")
    end_time = datetime.strptime(input_json["end"], "%Y-%m-%d %H:%M:%S.%f")
    combined = AudioSegment.silent(duration=0)

    output_segments = []
    end_time_tmp = begin_time

    trim_tmp = 0
    for item in input_json['list']:
        file_path = item['filePath']
        print(f"file_path:{file_path}")
        begin_time_item = datetime.strptime(item['beginTime'], '%Y-%m-%d %H:%M:%S.%f') # 语音片段开始时间
        audio_item, audio_item_length_ms = download_audio_from_minio(file_path)
        # audio_item = AudioSegment.from_wav(file_path)
        # audio_item_length_ms = len(audio_item)

        if audio_item is None:
            return None, None
        print(f"audio_item_length_ms:{audio_item_length_ms}")
        end_time_item = begin_time_item + timedelta(milliseconds=audio_item_length_ms) # 语音片段结束时间

        if begin_time_item < end_time:
            start_trim = (end_time_tmp - begin_time_item).total_seconds() * 1000
            end_trim = min((end_time - begin_time_item).total_seconds() * 1000, audio_item_length_ms)
            print(f"start_trim:{start_trim}")
            print(f"end_trim:{end_trim}")
            if start_trim < 0:
                trim_tmp += start_trim
                start_trim = 0
            print(f"trim_tmp:{trim_tmp}")
            trimmed_audio = audio_item[start_trim:end_trim]
            combined += trimmed_audio
            end_time_tmp = end_time_item

        output_segments.append({
            "sourceUuid": item["sourceUuid"],
            "filePath": item["filePath"],
            "sampleMachine": item["sampleMachine"],
            "inputBeginTime": format_time(begin_time_item + timedelta(milliseconds=start_trim)),
            "inputEndTime": format_time(begin_time_item + timedelta(milliseconds=end_trim)),
            "outputBeginTime": format_time(begin_time_item + timedelta(milliseconds=start_trim + trim_tmp)),
            "outputEndTime": format_time(begin_time_item + timedelta(milliseconds=end_trim + trim_tmp)),
            "duration": int(end_trim - start_trim)
        })
        print()

    return combined, output_segments

def upload_audio_to_minio(audio, input_json):
    if audio is None: return
    client = Minio(
        input_json["uploadHost"],
        access_key = input_json["uploadUser"],
        secret_key = input_json["uploadPasswd"],
        secure = False
    )

    bucket_name = input_json["uploadBucket"]
    object_name = input_json["uploadObject"]
    buffer = BytesIO()
    audio.export(buffer, format="wav")
    buffer.seek(0)

    try:
        client.put_object(
            bucket_name,
            object_name,
            buffer,
            length=buffer.getbuffer().nbytes,
            content_type="audio/wav"
        )
        print(f"File {object_name} uploaded successfully.")
    except Exception as err:
        print(err)

def write_audio(audio, audio_path):
    audio.export(audio_path, format="wav")

def write_json(input_json, wav_list, output_path):
    output_json = {
        "begin": input_json["begin"],
        "end": input_json["end"],
        "filePath": f"http://{input_json['uploadHost']}/{input_json['uploadBucket']}/{input_json['uploadObject']}",
        "list": wav_list
    }

    with open(output_path, 'w') as json_file:
        json.dump(output_json, json_file, indent=4)

def _main():
    input_json = read_json("input.json")
    audio, wav_list = audio_selec(input_json)
    upload_audio_to_minio(audio, input_json)
    # write_audio(audio, "combined_audio.wav")
    write_json(input_json, wav_list, "output.json")

if __name__ == '__main__':
    _main()
