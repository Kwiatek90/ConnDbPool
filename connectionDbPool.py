import psycopg2
import config
import threading
from queue import Queue, Empty
import time  



class ConnectionPool():
    def __init__(self, database_config_path):
        self.database_config_path = database_config_path
        self.min_queue_conn = 10
        self.max_conn = 50
        self.queue = Queue(maxsize=self.max_conn)
        self.active_conn = 0  
        self.rel_conn = 0
        self.start_time = time.time()
        self.semaphore = threading.Semaphore(self.max_conn)  
        self.open_threads_connections()
        self.cleanup_thread = threading.Thread(target=self.cleanup_connections, daemon=True)
        self.cleanup_thread.start()
        self.check_conn = threading.Thread(target=self.check_conn, daemon=True)
        self.check_conn.start()
        
    def open_threads_connections(self):
        '''At the beginning create a 10 locked connections in queue'''
        
        with self.semaphore:
            for _ in range(self.min_queue_conn):
                self.queue.put(self.connect_db())
    
    def connect_db(self):
        """Connect to the PostgreSQL database server"""

        params = config.config_params(self.database_config_path)
        conn = psycopg2.connect(**params)
        return conn
        
    def get_conn(self):
        '''Take the connection from queue, if queue is empty, create one'''
        
        self.semaphore.acquire()
          
        try:
            conn_db = self.queue.get_nowait()
            self.active_conn += 1
        except Empty:
            if self.active_conn < self.max_conn:
                conn_db = self.connect_db()
                self.active_conn +=1
            else:  
                raise ValueError ("Too much connections!")
            
        return conn_db
                
    def release_conn(self, conn_db):
        '''Release connection to the queue'''
        
        self.queue.put(conn_db) 
        self.rel_conn += 1
        self.active_conn -= 1
        self.semaphore.release()


    def cleanup_connections(self):
        '''Every 60 seconds function check connections'''
        
        while True:
            time.sleep(60)
            with self.semaphore:
                while not self.queue.empty():
                    if self.queue.qsize() > self.min_queue_conn:
                        conn_db = self.queue.get()
                        conn_db.close()
                        
                        
    def check_conn(self):
        while True:
            time.sleep(0.5)
            print(f"Active conn: {self.active_conn}\nRelease conn: {self.rel_conn}\nQueue size: {self.queue.qsize()}\n")
            