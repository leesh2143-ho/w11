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

@app.route('/api/view/increment/<int:post_id>', methods=['POST'])
def increment_view_count(post_id):
    if post_id != POST_ID:
        return jsonify({"error": "Invalid Post ID"}), 400

    db_conn = None
    final_count = 0

    try:
        # DB 연결 (성공 후 쓰기를 위해 미리 연결하거나, 루프 안에서 연결할 수도 있음)
        db_conn = pymysql.connect(**DB_CONFIG)
        db_cursor = db_conn.cursor()

        # [CAS 루프] 성공할 때까지 무한 반복
        while True:
            try:
                # 1. 감시 시작 (Redis Watch)
                with redis_client.pipeline() as pipe:
                    pipe.watch(CACHE_KEY)
                    
                    # 2. 값 읽기 (READ)
                    current_val = pipe.get(CACHE_KEY)
                    if current_val is None:
                        # 캐시가 비었으면 DB에서 초기값을 가져와야 안전함
                        db_cursor.execute("SELECT view_count FROM content WHERE id = %s", (post_id,))
                        row = db_cursor.fetchone()
                        read_count = row[0] if row else 0
                    else:
                        read_count = int(current_val)
                    
                    # 3. 값 수정 계산 (MODIFY)
                    new_count = read_count + 1
                    
                    # (불일치 유발을 위한 지연 시간)
                    time.sleep(DELAY_SECONDS)

                    # 4. Redis 저장 시도 (WRITE / Check-And-Set)
                    pipe.multi()
                    pipe.set(CACHE_KEY, new_count)
                    
                    # execute() 실행 순간, Redis는 그 사이에 누가 CACHE_KEY를 건드렸는지 확인
                    pipe.execute()
                    
                    # ====================================================
                    # [중요] 여기까지 에러 없이 왔다면, 내가 '경쟁에서 승리'한 것임.
                    # 이제 안심하고 DB에도 똑같은 값을 씀.
                    # ====================================================
                    
                    # 5. DB 업데이트
                    # 이미 Redis CAS를 통해 '순서'가 정해졌으므로, 
                    # DB에는 계산된 new_count를 그대로 덮어써도 안전함.
                    db_cursor.execute("UPDATE content SET view_count = %s WHERE id = %s", (new_count, post_id))
                    db_conn.commit()

                    final_count = new_count
                    logger.info(f"Success (CAS): Redis & DB updated to {new_count}")
                    
                    # 성공했으면 루프 탈출!
                    break

            except redis.WatchError:
                # [실패 시] 누군가 먼저 선수침 -> 재시도
                logger.warning("Conflict! Retrying CAS...")
                continue
                
    except Exception as e:
        logger.error(f"Error: {e}")
        if db_conn:
            db_conn.rollback()
        return jsonify({"error": str(e)}), 500
    finally:
        if db_conn:
            db_conn.close()

    return jsonify({
        "status": "success",
        "post_id": post_id,
        "final_view_count_reported": final_count
    })

if __name__ == '__main__':
    logger.info("Starting API Server with Full CAS (Redis + DB Write)...")
    redis_client.set(CACHE_KEY, 0)
    app.run(host='0.0.0.0', port=5000, threaded=True, debug=False)
