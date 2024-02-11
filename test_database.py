import unittest
import time
import connectionDbPool
import threading
import random
class DatabaseTests(unittest.TestCase):
    def setUp(self):
        self.connPool = connectionDbPool.ConnectionPool("database.ini")
        
    def test_crash_with_5000_query_to_databsae_using_threads(self):
        
        def worker(i):
            try:
                num_queries = random.randint(100, 500)
                query = "SELECT * FROM users;"
                conn_db = self.connPool.get_conn()
                
                with conn_db.cursor() as cur:
                    for _ in range(num_queries):
                        cur.execute(query)
                        response = cur.fetchall()
                    self.connPool.release_conn(conn_db)
                    #print(f'Thread number: {i}')
                    
            except Exception as e:
                pass
                #print(f"Thread not executed: {i}, error: {e}")
                
        start = time.perf_counter()
        
        threads = []
        
        for i in range(800):
            thread = threading.Thread(target=worker, args=(i,))
            thread.start()
            threads.append(thread)
            
        for thread in threads:
            thread.join()

        finish = time.perf_counter()
        print(f"Finished in {round(finish-start,2)} second(s)")


if __name__ == "__main__": 
    unittest.main()
        