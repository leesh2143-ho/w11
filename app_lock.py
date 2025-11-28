import pymysql
import redis
from flask import Flask, jsonify
import time
import logging
from threading import Lock  # [변경] Lock 모듈 임포트

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
DELAY_SECONDS = 0.05  # 50ms (락 때문에 이제는 이 시간이 누적되어 전체 성능 저하의 주범이 됨)

app = Flask(__name__)
redis_client = redis.Redis(**REDIS_CONFIG)

# [변경] 글로벌 락 객체 생성
# 이 자물쇠는 프로그램 전체에서 단 하나만 존재합니다.
global_lock = Lock()

logging.basicConfig(level=logging.INFO, format='%(asctime)s - (%(threadName)s) - %(message)s')
logger = logging.getLogger(__name__)

# --------------------
# 2. 핵심 API 로직
# --------------------
@app.route('/api/view/increment/<int:post_id>', methods=['POST'])
def increment_view_count(post_id):
    if post_id != POST_ID:
        return jsonify({"error": "Invalid Post ID"}), 400

    # [변경] 락 획득 (Global Lock 적용)
    # 이 'with' 블록 안에 들어온 스레드만 코드를 실행할 수 있습니다.
    # 이미 누군가 들어와 있다면, 그 사람이 나갈 때까지 대기합니다.
    with global_lock:
        db_conn = None
        try:
            # --- 여기서부터는 한 번에 한 명만 실행됨 (Single Thread 처럼 동작) ---
            
            db_conn = pymysql.connect(**DB_CONFIG)
            db_cursor = db_conn.cursor()

            # (1) 캐시에서 조회수 읽기
            current_count_str = redis_client.get(CACHE_KEY)
            
            db_count = 0
            if current_count_str is None:
                # (2) 캐시 미스 처리
                db_cursor.execute("SELECT view_count FROM content WHERE id = %s", (post_id,))
                row = db_cursor.fetchone()
                if row:
                    db_count = row[0]
                    redis_client.set(CACHE_KEY, db_count)
                read_count = db_count
            else:
                read_count = int(current_count_str)

            # (3) 조회수 +1 증가
            new_count = read_count + 1
            
            # (4) DB에 저장
            db_cursor.execute("UPDATE content SET view_count = view_count + 1 WHERE id = %s", (post_id,))
            db_conn.commit()
            
            # (5) 의도적인 지연 시간 (이제는 이 시간 동안 다른 스레드들도 줄 서서 기다려야 함)
            time.sleep(DELAY_SECONDS)
            
            # (6) 캐시에 저장
            redis_client.set(CACHE_KEY, new_count)
            
            logger.info(f"Updated: {read_count} -> {new_count}")

            return jsonify({
                "status": "success",
                "post_id": post_id,
                "final_view_count_reported": new_count
            })

        except Exception as e:
            logger.error(f"Error occurred: {e}", exc_info=True)
            if db_conn:
                try:
                    db_conn.rollback()
                except Exception as rb_e:
                    logger.error(f"Rollback failed: {rb_e}")
            return jsonify({"error": str(e)}), 500
        finally:
            if db_conn:
                db_conn.close()
    # [변경] with 블록이 끝나면 자동으로 락이 반납(Release)되고, 기다리던 다음 스레드가 진입합니다.

if __name__ == '__main__':
    logger.info("Starting API Server with Global Lock...")
    redis_client.set(CACHE_KEY, 0)
    app.run(host='0.0.0.0', port=5000, threaded=True, debug=False)
