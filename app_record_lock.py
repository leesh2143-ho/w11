import pymysql
import redis
from flask import Flask, jsonify
import time
import logging
from threading import Lock
from collections import defaultdict

# --------------------
# 1. 설정 (Configuration)
# --------------------
DB_CONFIG = {
    "user": "w11",
    "password": "q1w2e3r4",
    "host": "127.0.0.1",
    "database": "w11_exam",
    "autocommit": False
}

REDIS_CONFIG = {
    "host": "127.0.0.1",
    "port": 6379,
    "decode_responses": True
}

POST_ID = 1
CACHE_KEY = f"post:{POST_ID}:view_count"
DELAY_SECONDS = 0.05 

app = Flask(__name__)
redis_client = redis.Redis(**REDIS_CONFIG)

# [변경] ID별 락 관리자
# post_locks[1] 은 1번 게시글 전용 락, post_locks[2]는 2번 전용 락...
# defaultdict를 사용하여 새로운 ID가 들어오면 자동으로 락을 생성합니다.
post_locks = defaultdict(Lock)

logging.basicConfig(level=logging.INFO, format='%(asctime)s - (%(threadName)s) - %(message)s')
logger = logging.getLogger(__name__)

# --------------------
# 2. 핵심 API 로직
# --------------------
@app.route('/api/view/increment/<int:post_id>', methods=['POST'])
def increment_view_count(post_id):
    if post_id != POST_ID:
        return jsonify({"error": "Invalid Post ID"}), 400

    # [변경] 해당 게시글 ID에 맞는 락을 가져옴
    # 현재 테스트는 모두 post_id=1 이므로, 모두가 같은 락 객체를 사용하게 됨
    current_lock = post_locks[post_id]

    # [변경] ID 전용 락 획득
    # ID가 서로 다르면 동시에 실행되지만, ID가 같으면 대기해야 함.
    with current_lock:
        db_conn = None
        try:
            db_conn = pymysql.connect(**DB_CONFIG)
            db_cursor = db_conn.cursor()

            # (1) 캐시 읽기
            current_count_str = redis_client.get(CACHE_KEY)
            
            db_count = 0
            if current_count_str is None:
                db_cursor.execute("SELECT view_count FROM content WHERE id = %s", (post_id,))
                row = db_cursor.fetchone()
                if row:
                    db_count = row[0]
                    redis_client.set(CACHE_KEY, db_count)
                read_count = db_count
            else:
                read_count = int(current_count_str)

            # (2) 증가
            new_count = read_count + 1
            
            # (3) DB 쓰기
            db_cursor.execute("UPDATE content SET view_count = view_count + 1 WHERE id = %s", (post_id,))
            db_conn.commit()
            
            # (4) 지연 (병목 구간)
            time.sleep(DELAY_SECONDS)
            
            # (5) 캐시 쓰기
            redis_client.set(CACHE_KEY, new_count)
            
            logger.info(f"Updated Post {post_id}: {read_count} -> {new_count}")

            return jsonify({
                "status": "success",
                "post_id": post_id,
                "final_view_count_reported": new_count
            })

        except Exception as e:
            logger.error(f"Error: {e}", exc_info=True)
            if db_conn:
                try: db_conn.rollback()
                except: pass
            return jsonify({"error": str(e)}), 500
        finally:
            if db_conn:
                db_conn.close()

if __name__ == '__main__':
    logger.info("Starting API Server with Fine-grained (ID-level) Lock...")
    redis_client.set(CACHE_KEY, 0)
    app.run(host='0.0.0.0', port=5000, threaded=True, debug=False)
