import redis
from apscheduler.schedulers.background import BackgroundScheduler
class User:
    def __init__(self):
        self.red = redis.Redis(host='localhost', port=6379, db=1)
        self.stream = 'customer'
        self.record_processed_key = 'record_processed'      

    def process_stream(self):
        try:      
            read_field = 'read'
            processed_field = 'processed'

            stream_data = self.red.xrevrange(self.stream, '+', '-', count=1)

            for entry_id, entry in stream_data:
                customer_id = entry[b'id '].decode()
                customer_name = entry[b'name '].decode()

                if self.red.get(self.record_processed_key) and self.red.get(self.record_processed_key).decode() == '1':
                    print(f"Customer ID: {customer_id}, Customer Name: {customer_name}")
                    self.red.xdel(self.stream, entry_id)
                    print("Record already processed and deleted")
                else:
                    print(f"Customer ID: {customer_id}, Customer Name: {customer_name}")

                    self.red.set(self.record_processed_key, '1')
                self.last_processed_id = entry_id.decode()
        

        except Exception as e:
            print(f"Error processing stream: {e}")

if __name__ == "__main__":
    user = User()
    scheduler = BackgroundScheduler()
    scheduler.add_job(user.process_stream, trigger='interval', seconds=5)
    scheduler.start()

    try:
        while True:
            pass
    except (KeyboardInterrupt, SystemExit):
        scheduler.shutdown()