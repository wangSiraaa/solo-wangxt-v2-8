package com.gameops.craft;

import static org.assertj.core.api.Assertions.assertThat;

import com.gameops.craft.support.TestDataResetter;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.client.TestRestTemplate;
import org.springframework.boot.test.web.server.LocalServerPort;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpMethod;
import org.springframework.http.ResponseEntity;
import org.springframework.test.annotation.DirtiesContext;

/**
 * Black-box HTTP coverage for the emergency recall batch:
 * operator initiates (idempotent), batch liquidates, player sees recalled status
 * and the full paired material trail, repeat initiation returns the same batch,
 * and a post-recall preoccupy is refused by the server.
 */
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@DirtiesContext(classMode = DirtiesContext.ClassMode.AFTER_CLASS)
class RecallApiTest {

    @LocalServerPort int port;
    @Autowired TestRestTemplate rest;
    @Autowired TestDataResetter resetter;

    private String base;
    private String opsToken;
    private String p3Token;

    @BeforeEach
    void setUp() {
        rest.getRestTemplate().setRequestFactory(
                new org.springframework.http.client.JdkClientHttpRequestFactory());
        resetter.reset();
        base = "http://localhost:" + port;
        opsToken = login("ops_admin", "operator123");
        p3Token = login("player3", "player123");
    }

    private String login(String u, String p) {
        var r = rest.postForEntity(base + "/api/auth/login", Map.of("username", u, "password", p), Map.class);
        assertThat(r.getStatusCode().is2xxSuccessful()).isTrue();
        return (String) r.getBody().get("token");
    }

    private HttpHeaders auth(String token, String idemKey) {
        HttpHeaders h = new HttpHeaders();
        h.set("X-Auth-Token", token);
        if (idemKey != null) {
            h.set("Idempotency-Key", idemKey);
        }
        return h;
    }

    @Test
    void operator_recalls_version_batch_liquidates_and_player_sees_recall_trail() {
        // One preoccupied (left open) and one committed FIRE_SWORD order for player3.
        String pre = (String) rest.exchange(base + "/api/player/crafts/preoccupy", HttpMethod.POST,
                new HttpEntity<>(Map.of("recipeId", 1), auth(p3Token, UUID.randomUUID().toString())), Map.class)
                .getBody().get("orderNo");
        String done = (String) rest.exchange(base + "/api/player/crafts/preoccupy", HttpMethod.POST,
                new HttpEntity<>(Map.of("recipeId", 1), auth(p3Token, UUID.randomUUID().toString())), Map.class)
                .getBody().get("orderNo");
        rest.exchange(base + "/api/player/crafts/commit", HttpMethod.POST,
                new HttpEntity<>(Map.of("orderNo", done), auth(p3Token, UUID.randomUUID().toString())), Map.class);

        // Operator initiates recall for version 1.
        String recallKey = UUID.randomUUID().toString();
        var init = rest.exchange(base + "/api/operator/recalls", HttpMethod.POST,
                new HttpEntity<>(Map.of("versionId", 1, "reason", "配置错误"), auth(opsToken, recallKey)), Map.class);
        assertThat(init.getStatusCode().is2xxSuccessful()).isTrue();
        String batchNo = (String) init.getBody().get("batchNo");
        assertThat(init.getBody().get("created")).isEqualTo(true);
        assertThat(((Number) init.getBody().get("totalOrders"))).isEqualTo(2);

        // Drive the batch, then verify persisted progress and per-result counts.
        var run = rest.exchange(base + "/api/operator/recalls/" + batchNo + "/run",
                HttpMethod.POST, new HttpEntity<>(auth(opsToken, null)), Map.class);
        assertThat(run.getStatusCode().is2xxSuccessful()).isTrue();
        assertThat(run.getBody().get("status")).isEqualTo("COMPLETED");

        var detail = rest.exchange(base + "/api/operator/recalls/" + batchNo,
                HttpMethod.GET, new HttpEntity<>(auth(opsToken, null)), Map.class).getBody();
        Map<String, Object> counts = (Map<String, Object>) detail.get("resultCounts");
        assertThat(counts.get("RELEASED")).isEqualTo(1);
        assertThat(counts.get("REVERSED")).isEqualTo(1);
        List<Map<String, Object>> orders = (List<Map<String, Object>>) detail.get("orders");
        assertThat(orders).hasSize(2);
        assertThat(orders).allSatisfy(o -> {
            assertThat(o.get("status")).isEqualTo("DONE");
            assertThat(o.get("detail")).isNotNull();
        });

        // Repeat initiation (same key, and again with a new key) returns the SAME batch.
        var repeat = rest.exchange(base + "/api/operator/recalls", HttpMethod.POST,
                new HttpEntity<>(Map.of("versionId", 1), auth(opsToken, recallKey)), Map.class);
        assertThat(repeat.getBody().get("batchNo")).isEqualTo(batchNo);
        assertThat(repeat.getBody().get("replayed")).isEqualTo(true);
        var repeat2 = rest.exchange(base + "/api/operator/recalls", HttpMethod.POST,
                new HttpEntity<>(Map.of("versionId", 1), auth(opsToken, UUID.randomUUID().toString())), Map.class);
        assertThat(repeat2.getBody().get("batchNo")).isEqualTo(batchNo);

        // Running again is a no-op continuation (no double compensation).
        var runAgain = rest.exchange(base + "/api/operator/recalls/" + batchNo + "/run",
                HttpMethod.POST, new HttpEntity<>(auth(opsToken, null)), Map.class);
        assertThat(runAgain.getBody().get("processedOrders")).isEqualTo(2);

        // New preoccupy on the recalled version is refused.
        var blocked = rest.exchange(base + "/api/player/crafts/preoccupy", HttpMethod.POST,
                new HttpEntity<>(Map.of("recipeId", 1), auth(p3Token, UUID.randomUUID().toString())), Map.class);
        assertThat(blocked.getStatusCode().value()).isEqualTo(409);
        assertThat(blocked.getBody().get("error")).isEqualTo("VERSION_RECALLED");

        // Player sees the recalled status and the full paired material trail on the committed order.
        var doneDetail = rest.exchange(base + "/api/player/crafts/" + done,
                HttpMethod.GET, new HttpEntity<>(auth(p3Token, null)), Map.class).getBody();
        assertThat(doneDetail.get("status")).isEqualTo("RECALLED");
        assertThat(doneDetail.get("recalled")).isEqualTo(true);
        assertThat(doneDetail.get("recallBatchNo")).isEqualTo(batchNo);
        List<Map<String, Object>> ledger = (List<Map<String, Object>>) doneDetail.get("ledger");
        assertThat(ledger).extracting(e -> e.get("entryType"))
                .contains("CONSUME", "PRODUCE", "RECALL_CLAWBACK", "RECALL_RETURN");

        var preDetail = rest.exchange(base + "/api/player/crafts/" + pre,
                HttpMethod.GET, new HttpEntity<>(auth(p3Token, null)), Map.class).getBody();
        assertThat(preDetail.get("status")).isEqualTo("RECALLED");
        List<Map<String, Object>> preLedger = (List<Map<String, Object>>) preDetail.get("ledger");
        assertThat(preLedger).extracting(e -> e.get("entryType")).contains("CONSUME", "RELEASE");
    }

    @Test
    void output_shortfall_goes_to_retryable_exception_queue_and_succeeds_after_grant() {
        String orderNo = (String) rest.exchange(base + "/api/player/crafts/preoccupy", HttpMethod.POST,
                new HttpEntity<>(Map.of("recipeId", 1), auth(p3Token, UUID.randomUUID().toString())), Map.class)
                .getBody().get("orderNo");
        rest.exchange(base + "/api/player/crafts/commit", HttpMethod.POST,
                new HttpEntity<>(Map.of("orderNo", orderNo), auth(p3Token, UUID.randomUUID().toString())), Map.class);
        // Spend the 100 GOLD reward.
        rest.exchange(base + "/api/operator/inventory/grant", HttpMethod.POST,
                new HttpEntity<>(Map.of("playerId", 4, "itemCode", "GOLD", "qty", 0), auth(opsToken, null)), Map.class);

        var init = rest.exchange(base + "/api/operator/recalls", HttpMethod.POST,
                new HttpEntity<>(Map.of("versionId", 1, "reason", "bad"), auth(opsToken, UUID.randomUUID().toString())),
                Map.class);
        String batchNo = (String) init.getBody().get("batchNo");
        rest.exchange(base + "/api/operator/recalls/" + batchNo + "/run",
                HttpMethod.POST, new HttpEntity<>(auth(opsToken, null)), Map.class);

        var exceptions = rest.exchange(base + "/api/operator/recall-exceptions?batchNo=" + batchNo,
                HttpMethod.GET, new HttpEntity<>(auth(opsToken, null)), List.class);
        assertThat(exceptions.getBody()).hasSize(1);
        assertThat(((Map<?, ?>) exceptions.getBody().get(0)).get("orderNo")).isEqualTo(orderNo);

        // Replenish the consumed output, then retry the parked order.
        rest.exchange(base + "/api/operator/inventory/grant", HttpMethod.POST,
                new HttpEntity<>(Map.of("playerId", 4, "itemCode", "GOLD", "qty", 100), auth(opsToken, null)), Map.class);
        var retry = rest.exchange(base + "/api/operator/recalls/retry", HttpMethod.POST,
                new HttpEntity<>(Map.of("batchNo", batchNo, "orderNo", orderNo), auth(opsToken, null)), Map.class);
        assertThat(retry.getStatusCode().is2xxSuccessful()).isTrue();
        assertThat(retry.getBody().get("result")).isEqualTo("REVERSED");

        var after = rest.exchange(base + "/api/operator/recalls/" + batchNo,
                HttpMethod.GET, new HttpEntity<>(auth(opsToken, null)), Map.class);
        assertThat(after.getBody().get("status")).isEqualTo("COMPLETED");
    }
}
