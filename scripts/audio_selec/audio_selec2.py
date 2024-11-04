import os
import logging
from minio import Minio
from io import BytesIO
from pydub import AudioSegment
from urllib.parse import urlparse
from datetime import datetime, timedelta
from flask import Flask, request, jsonify

app = Flask(__name__)

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def download_audio_from_minio(minio_url):
    parsed_url = urlparse(minio_url)
    path_parts = parsed_url.path.lstrip("/").split("/", 1)
    if len(path_parts) < 1:
        logging.error(f"Error: No bucket name found in the URL.")
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
        logging.error(f"Error downloading audio: {e}")
        return None, 0

def datetime_to_str(dt):
    return f"{dt:%Y-%m-%d %H:%M:%S}.{dt.microsecond//1000:03d}"

def str_to_datetime(str):
    return datetime.strptime(str, "%Y-%m-%d %H:%M:%S.%f")

def get_audio_length(file_path):
    return len(AudioSegment.from_wav(file_path))

def audio_selec(input_json):
    elements = [input_json["list"][0]]
    _, audio_length = download_audio_from_minio(input_json["list"][0]["filePath"])
    max_allowed_time = str_to_datetime(input_json["list"][0]["beginTime"]) + timedelta(milliseconds=audio_length)
    
    for i in range(1, len(input_json["list"]) - 1):
        if str_to_datetime(input_json["list"][i]["beginTime"]) < max_allowed_time:
            i += 1
        else:
            elements.append(input_json["list"][i-1])
            _, audio_length = download_audio_from_minio(input_json["list"][i]["filePath"])
            max_allowed_time = str_to_datetime(input_json["list"][i]["beginTime"]) + timedelta(milliseconds=audio_length)
    elements.append(input_json["list"][-1])

    begin_time = str_to_datetime(input_json["begin"])
    end_time = str_to_datetime(input_json["end"])
    combined = AudioSegment.silent(duration=0)

    output_segments = []
    end_time_tmp = begin_time

    trim_tmp = 0
    begin_sound = 0
    for item in elements:
        file_path = item["filePath"]
        logging.info(f"file_path:{file_path}")
        begin_time_item = str_to_datetime(item["beginTime"]) # audio segment begin

        audio_item, audio_item_length_ms = download_audio_from_minio(file_path)
        # audio_item = AudioSegment.from_wav(file_path)
        # audio_item_length_ms = len(audio_item)

        if audio_item is None:
            return None, None
        logging.info(f"segment begin time:{datetime_to_str(begin_time_item)}")
        logging.info(f"audio segment length:{audio_item_length_ms}ms")
        end_time_item = begin_time_item + timedelta(milliseconds=audio_item_length_ms) # audio segment end
        logging.info(f"segment end time:{datetime_to_str(end_time_item)}")

        if begin_time_item < end_time:
            start_trim = (end_time_tmp - begin_time_item).total_seconds() * 1000
            end_trim = min((end_time - begin_time_item).total_seconds() * 1000, audio_item_length_ms)
            if start_trim < 0:
                logging.info(f"sil before segment:{-1*start_trim:.0f}ms")
                trim_tmp += start_trim
                start_trim = 0
            if start_trim >= end_trim:
                continue
            trimmed_audio = audio_item[start_trim:end_trim]
            combined += trimmed_audio
            end_time_tmp = end_time_item
            logging.info(f"all sil before segment:{-1*trim_tmp:.0f}ms\n")

        output_segments.append({
            "sourceUuid": item["sourceUuid"],
            "filePath": item["filePath"],
            "sampleMachine": item["sampleMachine"],
            "inputBeginTime": datetime_to_str(begin_time_item+timedelta(milliseconds=start_trim)),
            "inputEndTime": datetime_to_str(begin_time_item+timedelta(milliseconds=end_trim)),
            "outputSecond": int((begin_time_item - begin_time).seconds*1000 + start_trim + trim_tmp),
            "beginSound": begin_sound,
            "duration": int(end_trim - start_trim)
        })
        begin_sound += int(end_trim - start_trim)

    return combined, output_segments

# upload to MinIO
def upload_audio_to_minio(audio, input_json):
    if audio is None:
        return {"error": "No audio to upload"}
    client = Minio(
        input_json["uploadHost"],
        access_key=input_json["uploadUser"],
        secret_key=input_json["uploadPasswd"],
        secure=False
    )
    
    bucket_name = input_json["uploadBucket"]
    object_name = input_json["uploadObject"]
    buffer = BytesIO()
    audio.export(buffer, format="wav")
    buffer.seek(0)
    length=buffer.getbuffer().nbytes
    input_json["fileSize"] = length

    try:
        client.put_object(
            bucket_name,
            object_name,
            buffer,
            length=buffer.getbuffer().nbytes,
            content_type="audio/wav"
        )
        logging.info(f"File {object_name} uploaded successfully.")
        return {"message": "File uploaded successfully", "object_name": object_name}
    except Exception as err:
        logging.error(err)
        return {"error": str(err)}

def write_audio(audio, audio_path):
    if os.path.dirname(audio_path) and not os.path.exists(os.path.dirname(audio_path)):
        os.makedirs(os.path.dirname(audio_path), exist_ok=True)
    audio.export(audio_path, format="wav")

# POST:process aduio and upload to MinIO
@app.route("/process_audio", methods=["POST"])
def process_audio():
    input_json = request.json
    if not input_json:
        return jsonify({"error": "Invalid input, JSON data required"}), 400
    input_json["list"].sort(key=lambda x: str_to_datetime(x["beginTime"]))

    audio, wav_list = audio_selec(input_json)
    if audio is None:
        return jsonify({"error": "Audio processing failed"}), 500
    
    upload_result = upload_audio_to_minio(audio, input_json)
    if "error" in upload_result:
        return jsonify(upload_result), 500
    # write_audio(audio, input_json["uploadObject"])
    
    output_json = {
        "begin": input_json["begin"],
        "end": input_json["end"],
        "fileDuration": len(audio),
        "fileSize": input_json["fileSize"],
        "filePath": f"http://{input_json['uploadHost']}/{input_json['uploadBucket']}/{input_json['uploadObject']}",
        "list": wav_list,
        "upload_result": upload_result
    }

    return jsonify(output_json), 200

if __name__ == "__main__":
    app.run(host='0.0.0.0', port=5000, debug=True)
