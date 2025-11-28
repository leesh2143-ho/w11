import pymysql
import redis
from flask import Flask, jsonify
import time
import logging

# --------------------
# 1. 설정
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

logging.basicConfig(level=logging.INFO, format='%(asctime)s - (%(threadName)s) - %(message)s')
logger = logging.getLogger(__name__)

# --------------------
# 2. Write-Through (Update DB -> Update Redis)
# --------------------
@app.route('/api/view/increment/<int:post_id>', methods=['POST'])
def increment_view_count(post_id):
    if post_id != POST_ID:
        return jsonify({"error": "Invalid Post ID"}), 400

    db_conn = None
    final_count = 0

    try:
        db_conn = pymysql.connect(**DB_CONFIG)
        db_cursor = db_conn.cursor()

        # (1) DB 업데이트
        # DB는 안전하게 1 증가
        db_cursor.execute("UPDATE content SET view_count = view_count + 1 WHERE id = %s", (post_id,))
        db_conn.commit()

        # (2) 불일치 유발 시간 (테스트용)
        time.sleep(DELAY_SECONDS)

        # (3) DB에서 최신 값 가져오기
        # 캐시에 넣을 '정확한 값'을 알기 위해 DB를 다시 조회합니다.
        db_cursor.execute("SELECT view_count FROM content WHERE id = %s", (post_id,))
        row = db_cursor.fetchone()
        if row:
            final_count = row[0]

            # (4) 캐시 업데이트 (DELETE가 아니라 SET)
            # 이제 Redis에도 값이 기록됩니다!
            redis_client.set(CACHE_KEY, final_count)
            
            logger.info(f"DB Updated to {final_count} -> Redis SET Complete")

        return jsonify({
            "status": "success",
            "post_id": post_id,
            "final_view_count_reported": final_count
        })

    except Exception as e:
        logger.error(f"Error: {e}")
        if db_conn:
            try: db_conn.rollback()
            except: pass
        return jsonify({"error": str(e)}), 500
    finally:
        if db_conn:
            db_conn.close()

if __name__ == '__main__':
    logger.info("Starting API Server with Write-Through (Redis UPDATE)...")
    redis_client.set(CACHE_KEY, 0)
    app.run(host='0.0.0.0', port=5000, threaded=True, debug=False)
