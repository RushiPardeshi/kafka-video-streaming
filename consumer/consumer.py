# import time
# from flask import Flask, render_template, Response
# from confluent_kafka import Consumer, KafkaError
# from PIL import Image
# from io import BytesIO
# import cv2
# import numpy as np
# from threading import Thread
# import torch

# app = Flask(__name__)

# # Load YOLOv5 model
# model = torch.hub.load('ultralytics/yolov5', 'yolov5s', pretrained=True)

# # Store the latest frame received
# latest_frame = None

# # Kafka Consumer Configuration
# conf = {
#     'bootstrap.servers': 'localhost:9092',
#     'group.id': 'my-group-id',
#     'auto.offset.reset': 'earliest'
# }

# consumer = Consumer(conf)
# topic = "kafka-video-topic"
# consumer.subscribe([topic])

# def kafka_consumer():
#     global latest_frame
#     try:
#         while True:
#             msg = consumer.poll(timeout=1.0)
#             if msg is None:
#                 continue
#             if msg.error():
#                 if msg.error().code() == KafkaError._PARTITION_EOF:
#                     print('%% %s [%d] reached end at offset %d\n' % (msg.topic(), msg.partition(), msg.offset()))
#                 else:
#                     raise KafkaError(msg.error())
#             else:
#                 video_frame_data = msg.value()
#                 image = Image.open(BytesIO(video_frame_data))
#                 image_np = np.array(image)

#                 # Convert RGB to BGR
#                 image_np = cv2.cvtColor(image_np, cv2.COLOR_RGB2BGR)

#                 # Perform detection
#                 results = model(image_np)

#                 # Render results on the image
#                 for result in results.xyxy[0]:
#                     cv2.rectangle(image_np, (int(result[0]), int(result[1])), (int(result[2]), int(result[3])), (255, 0, 0), 2)
                
#                 latest_frame = Image.fromarray(cv2.cvtColor(image_np, cv2.COLOR_BGR2RGB))
#     finally:
#         consumer.close()

# @app.route('/')
# def index():
#     return render_template('index.html')

# def generate():
#     global latest_frame
#     while True:
#         if latest_frame is not None:
#             img_io = BytesIO()
#             latest_frame.save(img_io, 'JPEG')
#             img_io.seek(0)
#             time.sleep(0.2)
#             yield (b'--frame\r\n'
#                    b'Content-Type: image/jpeg\r\n\r\n' + img_io.getvalue() + b'\r\n')
#         else:
#             yield (b'--frame\r\n'
#                    b'Content-Type: image/jpeg\r\n\r\n' + b'\r\n')

# @app.route('/read_video')
# def read_video():
#     return Response(generate(),
#                     mimetype='multipart/x-mixed-replace; boundary=frame')

# if __name__ == '__main__':
#     # Start Kafka consumer in a background thread
#     t = Thread(target=kafka_consumer)
#     t.daemon = True
#     t.start()

#     app.run(host='0.0.0.0', port=5000, debug=True, threaded=True)


import time
from flask import Flask, render_template, Response
from confluent_kafka import Consumer, KafkaError
from PIL import Image
from io import BytesIO
import cv2
import numpy as np
from threading import Thread
import torch

app = Flask(__name__)

# Load YOLOv5 model
model = torch.hub.load('ultralytics/yolov5', 'yolov5s', pretrained=True)

# Store the latest frame received
latest_frame = None

# Kafka Consumer Configuration
conf = {
    'bootstrap.servers': 'localhost:9092',
    'group.id': 'my-group-id',
    'auto.offset.reset': 'earliest'
}

consumer = Consumer(conf)
topic = "kafka-video-topic"
consumer.subscribe([topic])

def kafka_consumer():
    global latest_frame
    try:
        while True:
            msg = consumer.poll(timeout=1.0)
            if msg is None:
                continue
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    print('%% %s [%d] reached end at offset %d\n' % (msg.topic(), msg.partition(), msg.offset()))
                else:
                    raise KafkaError(msg.error())
            else:
                video_frame_data = msg.value()
                image = Image.open(BytesIO(video_frame_data))
                image_np = np.array(image)

                # Convert RGB to BGR
                image_np = cv2.cvtColor(image_np, cv2.COLOR_RGB2BGR)

                # Perform detection
                results = model(image_np)

                # Render results on the image
                for result in results.xyxy[0]:
                    # Extract label and confidence
                    label = f"{model.names[int(result[5])]}: {result[4]:.2f}"
                    cv2.rectangle(image_np, (int(result[0]), int(result[1])), (int(result[2]), int(result[3])), (255, 0, 0), 2)
                    cv2.putText(image_np, label, (int(result[0]), int(result[1])-10), cv2.FONT_HERSHEY_SIMPLEX, 0.5, (255, 255, 255), 2)

                latest_frame = Image.fromarray(cv2.cvtColor(image_np, cv2.COLOR_BGR2RGB))
    finally:
        consumer.close()

@app.route('/')
def index():
    return render_template('index.html')

def generate():
    global latest_frame
    while True:
        if latest_frame is not None:
            img_io = BytesIO()
            latest_frame.save(img_io, 'JPEG')
            img_io.seek(0)
            time.sleep(0.2)
            yield (b'--frame\r\n'
                   b'Content-Type: image/jpeg\r\n\r\n' + img_io.getvalue() + b'\r\n')
        else:
            yield (b'--frame\r\n'
                   b'Content-Type: image/jpeg\r\n\r\n' + b'\r\n')

@app.route('/read_video')
def read_video():
    return Response(generate(),
                    mimetype='multipart/x-mixed-replace; boundary=frame')

if __name__ == '__main__':
    # Start Kafka consumer in a background thread
    t = Thread(target=kafka_consumer)
    t.daemon = True
    t.start()

    app.run(host='0.0.0.0', port=5000, debug=True, threaded=True)
