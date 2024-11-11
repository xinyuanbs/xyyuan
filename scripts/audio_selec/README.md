curl -X POST http://127.0.0.1:5001/process_audio1 -H "Content-Type: application/json" -d @input.json
curl -X POST http://127.0.0.1:5002/process_audio2 -H "Content-Type: application/json" -d @input.json
curl -X POST http://127.0.0.1:5003/process_audio3 -H "Content-Type: application/json" -d @input.json