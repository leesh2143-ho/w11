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
# 2. Write-Through (Update DB -> Invalidate Cache)
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

        # (1) DB 업데이트 (Source of Truth)
        # 가장 중요한 원본 데이터를 먼저 안전하게 증가시킵니다.
        # DB의 Row Lock 덕분에 순차적으로 정확히 +1 됩니다.
        db_cursor.execute("UPDATE content SET view_count = view_count + 1 WHERE id = %s", (post_id,))
        db_conn.commit()

        # (2) 불일치 유발 시간 (테스트용)
        # 이 시간 동안 다른 스레드들이 DB를 더 업데이트 할 수 있습니다.
        time.sleep(DELAY_SECONDS)

        # (3) 캐시 무효화 (Invalidation)
        # [핵심] 값을 계산해서 redis_client.set() 하는 게 아니라, 그냥 지워버립니다.
        # 이렇게 하면 '순서 꼬임'으로 인한 덮어쓰기 문제가 원천 차단됩니다.
        redis_client.delete(CACHE_KEY)
        
        # (4) 응답을 위해 현재 DB 값 조회 (선택 사항)
        # 실제 API 응답을 위해 최신 값을 DB에서 다시 읽어옵니다.
        # (혹은 위 UPDATE 문 실행 시 리턴받을 수도 있음)
        db_cursor.execute("SELECT view_count FROM content WHERE id = %s", (post_id,))
        row = db_cursor.fetchone()
        final_count = row[0]
        
        logger.info(f"Updated DB to {final_count} and Deleted Cache")

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
    logger.info("Starting API Server with Write-Through (Cache Deletion)...")
    redis_client.set(CACHE_KEY, 0)
    app.run(host='0.0.0.0', port=5000, threaded=True, debug=False)
