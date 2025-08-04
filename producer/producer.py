import sys
import time
import cv2
# from kafka import KafkaProducer
from confluent_kafka import Producer

topic = "kafka-video-topic"
conf = {
    'bootstrap.servers': 'localhost:9092',
    'group.id': 'my-group-id',
    'auto.offset.reset': 'earliest'
}

producer = Producer(conf)

def publish_video(video_file):
    """
    Publish given video file to a specified Kafka topic. 
    Kafka Server is expected to be running on the localhost. Not partitioned.
    
    :param video_file: path to video file <string>
    """
    
    # Open file
    video = cv2.VideoCapture(video_file)
    
    print('publishing video...')

    while(video.isOpened()):
        success, frame = video.read()

        # Ensure file was read successfully
        if not success:
            print("bad read!")
            break
        
        # Convert image to png
        ret, buffer = cv2.imencode('.jpg', frame)

        # print("ret, buffer", ret, buffer)
        # Convert to bytes and send to kafka
        producer.produce(topic, buffer.tobytes())
        producer.flush()
        # time.sleep(0.2)

        # print(frame.shape)
        
    video.release()
    print('publish complete')


def publish_camera():
    """
    Publish camera video stream to specified Kafka topic.
    Kafka Server is expected to be running on the localhost. Not partitioned.
    """

    # Start up producer
    # producer = Producer(bootstrap_servers='localhost:9092')

    camera = cv2.VideoCapture(0)
    try:
        while(True):
            success, frame = camera.read()
        
            ret, buffer = cv2.imencode('.jpg', frame)
            # producer.send(topic, buffer.tobytes())
            producer.produce(topic, buffer.tobytes())
            producer.flush()
            
            # Choppier stream, reduced load on processor
            # time.sleep(0)

    except:
        print("\nExiting.")
        sys.exit(1)

    
    camera.release()


if __name__ == '__main__':
    """
    Producer will publish to Kafka Server a video file given as a system arg. 
    Otherwise it will default by streaming webcam feed.
    """
    # sys.argv = ["hi", r"http://[2603:7000:7e00:364f:7d05:f2e:127a:db6e]:8080"]
    sys.argv = ["hi", r'/Users/rushipardeshi/Downloads/iCloud Photos-12/1056.mp4']
    if(len(sys.argv) > 1):
        video_path = sys.argv[1]
        publish_video(video_path)
    else:
        print("publishing feed!")
        publish_camera()