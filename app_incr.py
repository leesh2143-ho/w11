import pymysql
import redis
from flask import Flask, jsonify
import time
import logging

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

logging.basicConfig(level=logging.INFO, format='%(asctime)s - (%(threadName)s) - %(message)s')
logger = logging.getLogger(__name__)

# --------------------
# 2. 핵심 API 로직 (원자적 연산 버전)
# --------------------
@app.route('/api/view/increment/<int:post_id>', methods=['POST'])
def increment_view_count(post_id):
    if post_id != POST_ID:
        return jsonify({"error": "Invalid Post ID"}), 400

    db_conn = None
    try:
        # [특징] Python 코드 레벨의 Lock(global_lock 등)이 없습니다.
        # 따라서 스레드들은 여기서 병목 없이 쭉쭉 진입합니다.

        db_conn = pymysql.connect(**DB_CONFIG)
        db_cursor = db_conn.cursor()

        # (1) Redis Atomic Increment
        # 읽기(Get)와 쓰기(Set)를 쪼개지 않고, "증가시켜(Incr)" 명령 하나로 처리합니다.
        # Redis는 싱글 스레드이므로 이 명령은 무조건 순차적으로 정확히 실행됩니다.
        # 리턴값은 증가된 후의 최신 값입니다.
        current_redis_count = redis_client.incr(CACHE_KEY)
        logger.info(f"Redis INCR Result: {current_redis_count}")

        # (2) DB Atomic Update
        # Python에서 값을 계산해서 넣는 것이 아니라(%s 사용 안 함),
        # DB 엔진에게 "현재 값에 1을 더해라"라고 쿼리로 명령합니다.
        # DB는 이 행(Row)을 업데이트하는 순간 자동으로 락을 걸어 충돌을 방지합니다.
        db_cursor.execute("UPDATE content SET view_count = view_count + 1 WHERE id = %s", (post_id,))
        db_conn.commit()
        
        # (3) 지연 시간 (테스트용)
        # 이제는 이 지연 시간이 있어도 데이터 정합성에 아무런 영향을 주지 않습니다.
        # 이미 Redis와 DB는 각자 알아서 1을 증가시켰기 때문입니다.
        # 다만 응답 속도(Latency)만 0.05초 늦어질 뿐입니다.
        time.sleep(DELAY_SECONDS)

        # (4) 결과 반환
        return jsonify({
            "status": "success",
            "post_id": post_id,
            "final_view_count_reported": current_redis_count
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
    logger.info("Starting API Server with Atomic Operations (Redis INCR)...")
    # 테스트 시작 전 0으로 초기화
    redis_client.set(CACHE_KEY, 0)
    # DB도 0으로 초기화한다고 가정 (실제 운영환경에선 조심해야 함)
    # conn = pymysql.connect(**DB_CONFIG)
    # cur = conn.cursor()
    # cur.execute("UPDATE content SET view_count = 0 WHERE id = %s", (POST_ID,))
    # conn.commit()
    # conn.close()
    
    app.run(host='0.0.0.0', port=5000, threaded=True, debug=False)
