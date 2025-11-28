import pymysql
import redis
from flask import Flask, jsonify
import time
import logging
from threading import Lock

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

# [핵심] 초기화(Cache Miss) 시점의 중복 DB 조회를 막기 위한 락
# 전체 로직을 잠그는 것이 아니라, '데이터 로딩' 순간만 잠급니다.
init_lock = Lock()

logging.basicConfig(level=logging.INFO, format='%(asctime)s - (%(threadName)s) - %(message)s')
logger = logging.getLogger(__name__)

# --------------------
# 2. 핵심 로직: Double-Checked Locking 적용
# --------------------
@app.route('/api/view/increment/<int:post_id>', methods=['POST'])
def increment_view_count(post_id):
    if post_id != POST_ID:
        return jsonify({"error": "Invalid Post ID"}), 400

    db_conn = None
    final_count = 0

    try:
        # ====================================================
        # [Step 1] Double-Checked Locking (초기값 안전 로딩)
        # ====================================================
        
        # 1-1. [Check 1] 락 없이 먼저 확인 (대부분 여기서 통과되어 성능 좋음)
        if not redis_client.exists(CACHE_KEY):
            
            # 1-2. 캐시가 없다면 락 획득 (줄 서기)
            with init_lock:
                # 1-3. [Check 2] 락 안에서 한 번 더 확인 (Double Check)
                # 내가 줄 서는 동안 앞사람이 채워놨을 수 있으므로 필수!
                if not redis_client.exists(CACHE_KEY):
                    
                    logger.info("Cache Miss! Loading from DB (Protected by DCL)")
                    
                    # 1-4. DB에서 초기값 로딩 (단 한 명만 실행됨)
                    # 여기서는 로딩을 위해 잠시 DB 연결
                    temp_conn = pymysql.connect(**DB_CONFIG)
                    temp_cursor = temp_conn.cursor()
                    temp_cursor.execute("SELECT view_count FROM content WHERE id = %s", (post_id,))
                    row = temp_cursor.fetchone()
                    init_count = row[0] if row else 0
                    temp_conn.close()
                    
                    # 1-5. 캐시 초기화
                    redis_client.set(CACHE_KEY, init_count)
                    logger.info(f"Cache Initialized to {init_count}")
        
        # ====================================================
        # [Step 2] 조회수 증가 (Atomic Operation)
        # ====================================================
        # 위 DCL 덕분에 캐시 키가 존재한다는 것이 100% 보장됨.
        # 이제 안전하게 원자적 증가(INCR) 실행.
        
        final_count = redis_client.incr(CACHE_KEY)
        
        # [Step 3] DB 비동기/동기 업데이트 (Write-Back or Atomic Update)
        # 여기서는 DB도 원자적 쿼리로 안전하게 증가
        db_conn = pymysql.connect(**DB_CONFIG)
        db_cursor = db_conn.cursor()
        
        db_cursor.execute("UPDATE content SET view_count = view_count + 1 WHERE id = %s", (post_id,))
        db_conn.commit()

        # (테스트를 위한 지연)
        time.sleep(DELAY_SECONDS)

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
    logger.info("Starting API Server with Double-Checked Locking Pattern...")
    
    # [테스트 환경 설정]
    # DCL 동작을 확인하기 위해, 시작할 때 캐시를 일부러 지웁니다.
    redis_client.delete(CACHE_KEY)
    logger.info("Cache cleared to test Double-Checked Locking.")
    
    app.run(host='0.0.0.0', port=5000, threaded=True, debug=False)
