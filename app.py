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
    # PyMySQL 설정 추가: 커밋을 수동으로 제어
    "autocommit": False
}

REDIS_CONFIG = {
    "host": "127.0.0.1",
    "port": 6379,
    "decode_responses": True
}

POST_ID = 1  # 조회수를 증가시킬 게시글 ID
CACHE_KEY = f"post:{POST_ID}:view_count"
# 고의적인 지연 시간 (초 단위)
DELAY_SECONDS = 0.05  # 50ms (불일치 유발 핵심)

app = Flask(__name__)
# Redis 연결은 요청과 무관하게 미리 설정
redis_client = redis.Redis(**REDIS_CONFIG)

# 로깅 설정
logging.basicConfig(level=logging.INFO, format='%(asctime)s - (%(threadName)s) - %(message)s')
logger = logging.getLogger(__name__)

# --------------------
# 2. 핵심 API 로직
# --------------------
@app.route('/api/view/increment/<int:post_id>', methods=['POST'])
def increment_view_count(post_id):
    if post_id != POST_ID:
        return jsonify({"error": "Invalid Post ID"}), 400

    db_conn = None
    try:
        # 요청마다 DB 연결 생성 및 사용 후 닫기 (연결 객체 공유 방지)
        db_conn = pymysql.connect(**DB_CONFIG)
        # 커서는 요청이 끝날 때 자동으로 닫힙니다.
        db_cursor = db_conn.cursor()

        # (1) 캐시에서 조회수 읽기
        current_count_str = redis_client.get(CACHE_KEY)
        logger.info(f"Step 1. Read Cache: Value={current_count_str}")
        
        db_count = 0 
        
        if current_count_str is None:
            # (2) 캐시 값이 없으면 DB에서 읽어 캐시에 저장
            db_cursor.execute("SELECT view_count FROM content WHERE id = %s", (post_id,))
            row = db_cursor.fetchone()
            if row:
                db_count = row[0]
                # 캐시 미스 발생 시 Redis에 초기값 설정
                redis_client.set(CACHE_KEY, db_count)
                logger.info(f"Step 2. Cache Miss: Loaded from DB and set Cache to {db_count}")
            
            read_count = db_count
        else:
            read_count = int(current_count_str)
            
        # (3) 조회수 +1 증가
        new_count = read_count + 1
        logger.info(f"Step 3. Increment: Read={read_count}, New={new_count}")

        # (4) DB에 저장 (Read-Modify-Write)
        # PyMySQL은 %s 플레이스홀더를 사용합니다.
        # DB에서 현재 값을 읽어 1 증가시키는 원자적 쿼리 사용 (Lost Update 유발)
        db_cursor.execute("UPDATE content SET view_count = view_count + 1 WHERE id = %s", (post_id,))
        db_conn.commit()
        logger.info(f"Step 4. DB Write COMPLETE. Wrote +1 increment.")

        # ===============================================
        # !!! 불일치 유발 핵심 구간 !!!
        # 이 구간에서 다른 스레드가 개입하여 값을 변경하도록 유도합니다.
        time.sleep(DELAY_SECONDS) 
        logger.info(f"Timing Gap COMPLETE. (Delay: {DELAY_SECONDS}s)")
        # ===============================================

        # (5) 캐시에 저장 (이전에 읽은 'old' 값(new_count)으로 캐시를 덮어씁니다)
        redis_client.set(CACHE_KEY, new_count)
        logger.info(f"Step 5. Cache Write COMPLETE. Wrote value: {new_count}")

        # (6) 최종 조회수 반환
        return jsonify({
            "status": "success",
            "post_id": post_id,
            "final_view_count_reported": new_count
        })

    except Exception as e:
        logger.error(f"Error occurred in thread: {e}", exc_info=True)
        if db_conn:
            # 오류 발생 시 롤백 시도
            try:
                db_conn.rollback()
            except Exception as rb_e:
                logger.error(f"Rollback failed: {rb_e}")
        # 오류 발생 시 500 응답
        return jsonify({"error": f"An unexpected error occurred: {e}"}), 500
    finally:
        if db_conn:
            # 연결 객체를 명확히 닫아 메모리 누수 및 충돌 방지
            db_conn.close()

if __name__ == '__main__':
    logger.info("Starting API Server...")
    
    # 캐시 초기값 설정 (테스트 시작 전에 항상 0으로 초기화)
    redis_client.set(CACHE_KEY, 0)
    logger.info(f"Redis cache initialized: {CACHE_KEY} = 0")
    
    # VM 외부에서 접근 가능하도록 설정, threaded=True로 멀티스레드 구동
    app.run(host='0.0.0.0', port=5000, threaded=True, debug=False)
