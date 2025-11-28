import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

public class ConcurrencyTester {
    // VM에 배포된 Python API 서버의 IP 주소와 포트를 설정합니다.
    private static final String API_URL = "http://172.16.249.144:5000/api/view/increment/1";
//    private static final String API_URL = "http://172.16.249.144:5000/content/1/view";
//    private static final String API_URL = "http://172.16.249.144:5000/view/1";
    // 테스트 조건 설정
    private static final int NUM_THREADS = 50;  // 동시 요청을 보낼 스레드 수
    private static final int CALLS_PER_THREAD = 100; // 스레드당 반복 호출 횟수
    private static final int TOTAL_EXPECTED_CALLS = NUM_THREADS * CALLS_PER_THREAD; // 총 예상 호출 횟수: 5,000

    // 실제로 성공한 API 호출 횟수를 기록합니다. (정확한 카운트를 위해 AtomicInteger 사용)
    private static final AtomicInteger successfulCalls = new AtomicInteger(0);

    public static void main(String[] args) {
        System.out.println("=================================================");
        System.out.println("  ❌ 캐시/DB 불일치 유발 테스트 시작");
        System.out.println("=================================================");
        System.out.println("테스트 조건:");
        System.out.println("  스레드 수: " + NUM_THREADS);
        System.out.println("  스레드당 호출 횟수: " + CALLS_PER_THREAD);
        System.out.println("  총 예상 호출 횟수 (정상 값): " + TOTAL_EXPECTED_CALLS);
        System.out.println("  API 주소: " + API_URL);
        System.out.println("-------------------------------------------------");

        // ExecutorService를 사용하여 스레드 풀을 생성합니다.
        ExecutorService executor = Executors.newFixedThreadPool(NUM_THREADS);
        long startTime = System.currentTimeMillis();

        // 50개의 스레드를 실행합니다.
        for (int i = 0; i < NUM_THREADS; i++) {
            executor.submit(new ViewCountCaller(i));
        }

        // 모든 작업이 완료될 때까지 대기합니다.
        executor.shutdown();
        try {
            // 최대 5분 동안 대기
            if (!executor.awaitTermination(5, TimeUnit.MINUTES)) {
                System.out.println("Warning: 일부 스레드가 시간 내에 완료되지 못했습니다.");
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }

        long endTime = System.currentTimeMillis();

        // 최종 결과 출력 및 검증
        System.out.println("\n=================================================");
        System.out.println("  ✅ 테스트 완료 결과");
        System.out.println("=================================================");
        System.out.println("1. 총 호출 시도 횟수: " + TOTAL_EXPECTED_CALLS);
        System.out.println("2. API 성공 응답 횟수: " + successfulCalls.get());
        System.out.println("3. 경과 시간: " + (endTime - startTime) + " ms");

        // 이 후, VM에서 직접 DB와 캐시 값을 조회하여 비교해야 합니다.
        System.out.println("\n🚨 다음 단계: VM에서 직접 DB와 Redis 최종 값을 조회하여 '불일치'를 확인하세요.");
        System.out.println("  - Redis 조회: GET post:1:view_count");
        System.out.println("  - MariaDB 조회: SELECT view_count FROM w11_exam.content WHERE id = 1;");
    }

    // API 호출 작업을 수행하는 Runnable 클래스
    private static class ViewCountCaller implements Runnable {
        private final int threadId;

        public ViewCountCaller(int threadId) {
            this.threadId = threadId;
        }

        @Override
        public void run() {
            for (int i = 0; i < CALLS_PER_THREAD; i++) {
                try {
                    // API 호출
                    URL url = new URL(API_URL);
                    HttpURLConnection conn = (HttpURLConnection) url.openConnection();
                    conn.setRequestMethod("POST");
                    conn.setDoOutput(true);

                    int responseCode = conn.getResponseCode();

                    if (responseCode == HttpURLConnection.HTTP_OK) {
                        successfulCalls.incrementAndGet();
                        // 응답 본문을 읽어 로그 출력
                        try (BufferedReader br = new BufferedReader(new InputStreamReader(conn.getInputStream()))) {
                            String response = br.readLine();
                            // System.out.println("Thread " + threadId + " Success: " + response);
                        }
                    } else {
                        System.err.println("Thread " + threadId + " Error: HTTP Response Code " + responseCode);
                    }
                    conn.disconnect();
                } catch (Exception e) {
                    System.err.println("Thread " + threadId + " Exception: " + e.getMessage());
                }
            }
        }
    }
}
