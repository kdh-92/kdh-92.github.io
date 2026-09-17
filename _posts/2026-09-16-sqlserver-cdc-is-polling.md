---
layout: post
title: "CDC는 폴링이 아니다"가 SQL Server에서는 반만 맞는 이유
date: 2026-09-16
tags: [cdc, kafka, sqlserver, 아키텍처, 이벤트연계]
summary: SQL Server CDC 는 로그 직독이 아니라 캡처 잡과 커넥터로 이어지는 2단 폴링이다. 기본 5초 지연이 설계를 뒤집었다.
---

> 결론부터 말하면 **SQL Server 의 CDC 는 로그를 직독하지 않는다.** 캡처 잡이 로그를 훑어 별도 테이블에 복사하고, 커넥터가 그 테이블을 조회하는 **2단 폴링**이다.
> 그래서 "CDC 는 실시간이고 부하가 적다"는 통념이 여기서는 성립하지 않는다. 캡처 잡의 기본 대기가 **5초**다.
> 외부 DB 에서 이벤트를 가져와야 하는데 지연 예산이 빡빡하다면, 기술을 고르기 전에 이 구조부터 확인하시라.

---

## 1. 문제: 3초 예산에 5초가 먼저 들어왔다

외부 시스템의 DB에서 이벤트를 실시간으로 가져와야 하는 일이 생겼다.
요구는 단순했다. **원본에 기록이 들어오면 3초 안에 관제 화면에 띄울 것.**

처음 받은 방향은 "CDC로 붙자"였고, 나도 그게 맞다고 생각하고 검토를 시작했다.
그런데 벤더 문서를 읽을수록 숫자가 맞지 않았다. **첫 구간에서만 5초가 나갔다.**

일주일쯤 파고든 뒤에 내린 결론은 **CDC를 쓰지 않는 것**이었다.
그 과정에서 CDC에 대해 내가 갖고 있던 상식 하나가 DB에 따라 성립하지 않는다는 걸 알게 됐다.

---

## 2. 배경: 내가 갖고 있던 통념

CDC를 검색하면 대체로 이런 설명이 나온다.

> **동작 방식**: 데이터베이스의 트랜잭션 로그를 직접 읽거나 트리거를 사용해, 데이터가 바뀔 때 그 변경 사항만 실시간으로 잡아냅니다.
> **장점**: 변경이 일어날 때만 즉시 반응하므로 실시간 처리가 가능합니다. 주기적인 전체 조회나 무거운 쿼리가 없어 원본 데이터베이스 부하가 적고 효율적입니다.

틀린 설명은 아니다. 다만 **일반론**이다.

그리고 내가 다뤄야 할 DB는 **SQL Server**였다.

---

## 3. CDC는 하나가 아니라 세 계열이다

위 설명을 다시 보면 "트랜잭션 로그를 직접 읽거나 **트리거를 사용해**"라고 두 가지를 묶어놨다.
그런데 실제 구현은 세 계열이고, SQL Server는 그 둘 중 어느 쪽도 아니다.

### 계열 A. 로그 직독

MySQL, PostgreSQL, Oracle이 여기에 속한다.

- MySQL은 Debezium이 **자신을 복제 서버처럼 등록해서** binlog 스트림을 받는다
- PostgreSQL은 logical replication slot으로 WAL을 스트리밍 받는다
- Oracle은 LogMiner로 redo log를 읽는다

공통점은 **DB에 SQL을 날리지 않는다**는 것이다.
로그가 흘러나오는 걸 받아 적는 구조다. 통념의 "실시간", "원본 부하 없음"이 온전히 성립한다.

### 계열 B. 트리거 기반

원본 테이블에 `AFTER INSERT` 트리거를 걸어 별도 테이블에 적재하는 방식이다.

문제가 명확하다.
트리거는 **원본 INSERT와 같은 트랜잭션 안에서 동기로 실행**된다.
즉 이력 테이블 쓰기가 끝나야 원본이 커밋된다. **원본 업무가 느려진다.**
그래서 요즘은 거의 쓰지 않는다.

### 계열 C. 로그를 중간 테이블로 복사한 뒤 조회

**SQL Server CDC가 여기다.** 그리고 통념의 설명에는 이 항목이 없다.

왜 SQL Server만 다르냐면, **외부에서 트랜잭션 로그에 붙을 공개 API가 없기 때문**이다.
MySQL의 복제 프로토콜 같은 게 없다. 그래서 이렇게 우회한다.

> 내부적으로 로그를 읽어서 **일반 테이블에 복사해줄 테니, 그 테이블을 조회해라.**

---

## 4. 그래서 폴링이 두 번 일어난다

구조를 그리면 이렇게 된다.

```
  ┌──────────────────────────────────────────────┐
  │  SQL Server                                   │
  │                                               │
  │   [원본 테이블]                                │
  │        ↓ INSERT 가 커밋되면                    │
  │   [트랜잭션 로그]                              │
  │        ↓                                      │
  │   ┌─────────────────────────┐                │
  │   │ SQL Server Agent        │   ★ 폴링 1     │
  │   │  캡처 잡 sp_cdc_scan    │   기본 5초      │
  │   └─────────────────────────┘                │
  │        ↓ 변경 내역을 복사해 적는다             │
  │   [cdc.<스키마>_<테이블>_CT]  변경 테이블      │
  └────────────────┬─────────────────────────────┘
                   │ SELECT (JDBC)    ★ 폴링 2
                   │ 기본 500ms
  ┌────────────────┴─────────────────────────────┐
  │  Kafka Connect + Debezium 커넥터              │
  └──────────────────────────────────────────────┘
```

별표가 두 개다. 벤더 문서로 하나씩 확인해봤다.

### 폴링 1: 캡처 잡

Debezium 공식 문서의 표현이다.

> "The agent **reads new change event records from the transaction log** and **replicates the event records to a change data table**.
> Between the time that a change is committed in the source table, and the time that the change appears in the corresponding change table, **there is always a small latency interval**."

그리고 그 간격의 기본값이 명시돼 있다.

> `pollinginterval`
> "Specifies the number of seconds that the capture agent waits between log scan cycles.
> A higher value reduces the load on the database host and increases latency.
> A value of `0` specifies no wait between scans.
> **The default value is `5`.**"

**기본 5초다.** 로그를 한 번 훑고 5초를 잔다.

### 폴링 2: Debezium 커넥터

섹션 제목부터가 "**How Debezium SQL Server connectors read change data tables**"이다.
읽는 대상이 로그가 아니라 change data table이라고 제목에 적혀 있다.

본문은 이렇다.

> "For each change table, the connector read all of the changes that were created **between the last stored maximum LSN and the current maximum LSN**."

그리고 `poll.interval.ms` 설명이 결정적이다.

> "the number of milliseconds that **the connector waits before it checks the database** for new change events" (기본값 `500`)

**"checks the database"**라고 적혀 있다. DB를 조회한다.

---

## 5. 그래서 정확한 표현은

> **SQL Server CDC에서 커넥터는 "폴링하지 않는" 게 아니라, "원본 테이블 대신 변경 테이블을 폴링한다."**

통념의 두 장점을 대조해보면 이렇게 된다.

### "변경이 일어날 때 즉시 반응하므로 실시간"

계열 A에서는 맞다. 밀리초 단위다.

**SQL Server는 기본 5초다.** 커밋된 변경이 변경 테이블에 나타나기까지 그만큼 걸린다.
커넥터가 아무리 자주 조회해도 변경 테이블에 없는 건 읽을 수 없다.

### "원본 DB 부하가 적고 효율적"

앞부분인 "원본 테이블을 전체 조회하지 않는다"는 맞다.

**뒷부분인 "부하가 적다"는 성립하지 않는다.** 벤더 문서가 직접 부정한다.

> "**Each time that the capture job agent queries the database for new event records, it increases the CPU load on the database host.**
> The additional load on the server can have a negative effect on overall database performance, and potentially reduce transaction efficiency, **especially during times of peak database use**."

캡처 잡도 결국 DB 안에서 도는 작업이고 CPU를 먹는다.
게다가 CDC를 켜면 새로 생기는 부하가 셋 더 있다.

- 변경 테이블에 대한 **쓰기 I/O**. 원본 INSERT 1건당 변경 테이블 INSERT 1건이 추가된다
- **디스크 증가**
- **정리 잡**이 주기적으로 오래된 행을 DELETE 하는 부하

**부하가 사라진 게 아니라 원본 테이블에서 시스템 전체로 옮겨간 것이다.**

그리고 대상이 테이블 하나이고 초당 수 건 규모라면, **인덱스가 걸린 단순 폴링보다 총 부하가 클 수도 있다.**
CDC의 부하 이점이 확실해지는 건 테이블이 수십 개이고 폴링하면 매번 풀스캔이 일어나는 상황이다.

---

## 6. 그럼 CDC의 진짜 값어치는 무엇인가

여기까지만 보면 "쓸 이유가 없다"로 들리는데, 공정하게 보면 실질적인 차이가 남아 있다.
다만 그게 **속도나 부하가 아니라 정확성**이다.

### (1) 놓치는 행이 없다

이게 가장 크다. 폴링에는 구조적 결함이 하나 있다.

폴링 쿼리를 `WHERE 등록시각 > [마지막으로_읽은_값]`으로 짰다고 하자.

```
t = 10.0   트랜잭션 A 시작. 등록시각 '10:00:00' 행 INSERT. 아직 커밋 안 함
t = 10.5   트랜잭션 B 시작. 등록시각 '10:00:00.5' 행 INSERT 후 즉시 커밋
t = 11.0   폴링. A 는 미커밋이라 안 보이고 B 만 읽힌다
           → 마지막 값을 '10:00:00.5' 로 갱신
t = 12.0   트랜잭션 A 가 드디어 커밋된다
t = 12.0   폴링. WHERE 등록시각 > '10:00:00.5'
           → A 의 '10:00:00' 은 조건에 안 걸린다
```

**A는 영원히 읽히지 않는다. 그리고 에러가 나지 않는다.**

원인은 **시각(또는 증가 컬럼)이 "행이 만들어진 순서"이지 "커밋된 순서"가 아니기 때문**이다.

CDC는 시각이 아니라 **LSN(Log Sequence Number)**, 즉 로그에 기록된 순서를 쓴다.

> "The connector sorts the changes that it reads in ascending order, based on the values of their **commit LSN and change LSN**.
> This sorting order ensures that the changes are replayed in **the same order in which they occurred in the database**."

A가 늦게 커밋됐다면 commit LSN도 뒤다. 그래서 빠지지 않는다.

### (2) UPDATE와 DELETE를 잡는다

증분 컬럼 폴링은 새로 들어온 행만 본다. 기존 행의 수정과 삭제는 영원히 모른다.

### (3) 변경 전 값을 준다

이벤트에 `before`와 `after`가 둘 다 들어 있다.

### (4) 트리거보다 낫다

캡처 잡은 원본 트랜잭션과 **별개로 비동기로** 돈다. 원본 INSERT를 느리게 만들지 않는다.
계열 B와 비교하면 이건 분명한 장점이다.

---

## 7. 판단 기준을 만들어보면

정리하면 이렇게 갈린다.

**CDC가 확실히 나은 경우**

- UPDATE나 DELETE를 잡아야 한다
- 한 건도 놓치면 안 되는 정산성 데이터다
- 대상 테이블이 많고, 폴링하면 매번 풀스캔이 일어난다
- 원본 테이블에 조회 부하를 절대 주면 안 된다

**폴링이 나은 경우**

- INSERT만 일어난다
- 지연 예산이 빡빡하다 (SQL Server CDC는 기본 5초를 먼저 까먹고 시작한다)
- 대상 DB의 소유권이 우리에게 없어 구성 변경 권한을 받기 어렵다
- 빨리 시작해야 한다

마지막 두 항목이 내 상황이었다.

SQL Server CDC를 켜려면 이게 전부 필요하다.

- SQL Server 2016 SP1 이상의 Standard 또는 Enterprise 에디션
- `sys.sp_cdc_enable_db` 와 `sys.sp_cdc_enable_table` 실행
- 실행 주체가 **`db_owner` 고정 데이터베이스 역할의 구성원**
- **SQL Server Agent 상시 기동** (멈추면 변경 테이블 적재가 중단되는데, 커넥터는 에러를 내지 않고 조용히 멈춘다)
- 변경 테이블이 쌓는 디스크 증가분 수용

그리고 지연을 줄이려고 `pollinginterval`을 낮추는 것은,
문서 표현대로라면 **그 DB 호스트의 CPU 부하를 올려달라는 요청**이다.

**이건 기술 난이도 문제가 아니라 권한과 책임 경계의 문제다.**
소유권이 없는 DB라면 항목 하나하나가 협상 대상이 된다.

---

## 8. 내가 내린 결론

요구사항을 다시 보니 **INSERT만 발생하는 데이터**였다.
그러면 CDC의 강점 세 개 중 두 개가 그대로 사라진다.

남는 건 "놓치는 행이 없다" 하나인데, 이건 폴링 쪽에서도 상당 부분 메울 수 있다.

```sql
SELECT ...
  FROM 원본테이블 WITH (NOLOCK)
 WHERE 등록시각 >= DATEADD(second, -10, [커서])
 ORDER BY 등록시각
```

두 가지가 들어 있다.

**`>=`를 쓴다.**
`>`를 쓰면 같은 시각을 가진 행이 여러 건일 때 뒤엣것을 놓친다.
초 단위 정밀도면 동시 발생이 충분히 가능하다.

**여유초만큼 뒤로 물러나서 읽는다.**
§5-(1)의 늦게 커밋된 트랜잭션이 다음 주기에 잡힌다.

당연히 같은 행을 여러 번 읽게 되므로 **멱등 처리가 필요하다.**
이벤트의 자연키로 `eventId`를 만들고 UNIQUE 제약으로 거른다.

> 그런데 **멱등 처리는 어느 방식을 택하든 어차피 필요하다.**
> Kafka도 근본적으로 at-least-once이고, CDC 커넥터도 재시작하면 중복이 온다.
> 즉 겹쳐 읽기의 비용은 "새로 생기는 부담"이 아니라 "어차피 있는 방어선을 한 번 더 쓰는 것"이다.

---

## 9. 그런데 왜 Kafka는 남겼나

여기가 이 검토에서 가장 중요했던 지점이다.

중간에 요구가 하나 추가됐다. **이 이벤트의 소비자가 2개에서 10개로 늘어날 예정**이라는 것이었다.

처음엔 "그럼 CDC를 다시 봐야 하나" 싶었는데, 따져보니 아니었다.
**층을 나눠야 한다.**

```
[수집 층]   원본 DB 에서 이벤트를 어떻게 꺼낼 것인가
                ↓
[배분 층]   꺼낸 이벤트를 소비자들에게 어떻게 나눠줄 것인가
```

**소비자가 10개로 늘어나는 것은 배분 층의 문제다. 수집 층과는 무관하다.**

- **배분 층**: 소비자가 1개에서 10개가 되면 판단이 완전히 뒤집힌다. 여기서 Kafka가 정당해진다
- **수집 층**: 소비자가 몇 개든 원본에서 꺼내는 방법은 같다. INSERT만 있다는 사실이 여전하므로 CDC는 여전히 탈락이다

Kafka 없이 소비자 10개를 처리하면 이렇게 된다.

- 각 소비자가 알아서 폴링하므로 **외부 DB 조회가 10배**가 된다
- **커서를 10벌** 관리해야 하고, 각자 다른 지점에 있다
- 접속 계정과 방화벽 경로가 **10벌** 필요하다
- 소비자를 추가하는 일이 **매번 외부 협의 사항**이 된다

Kafka를 넣으면 이렇게 된다.

- 폴러가 하나이므로 **외부 DB 조회는 초당 1회 그대로**다. 소비자가 100개가 돼도 같다
- 소비자 추가는 `group.id` 하나 만드는 일이다. 원본과 폴러는 아무것도 안 바뀐다
- 소비자마다 처리 속도가 달라도 각자 자기 오프셋으로 자기 속도로 읽는다
- 소비자 하나가 죽어도 이벤트가 사라지지 않는다

그래서 최종 구조가 이렇게 됐다.

```
[외부 DB] ←── SELECT (1초 주기 · 10초 겹쳐 읽기)
                    │
              [전용 폴러]  ← 여기서 eventId 부여
                    │ produce (key = 자연키)
                [Kafka]
                    │  각 소비자가 서로 다른 group.id 로 fetch
       ┌────────┬───┴────┬──────── ... ────────┐
   소비자1   소비자2   소비자3              소비자10
```

---

## 10. 왜 Kafka Connect의 JDBC Source Connector가 아닌가

"폴링을 할 거면 커넥터에 맡기면 되지 않나"가 당연한 질문이다. 실제로 진지하게 검토했다.

커넥터에는 정확히 이 문제를 겨냥한 설정이 있다.

> `timestamp.delay.interval.ms`
> "How long to wait after a row with certain timestamp appears before we include it in the result.
> You may choose to add some delay to **allow transactions with earlier timestamp to complete**.
> ... Every following execution will get data from the last time we fetched **until current time minus the delay**."
> Default: **0**

**"current time minus the delay"** 가 핵심이다.

- `delay = 0` (기본값): 지연은 없지만 **늦게 커밋된 트랜잭션을 영구히 놓친다**
- `delay = 5000`: 놓침은 막지만 **지연이 5초 추가된다**

즉 커넥터의 겹쳐 읽기는 **앞을 잘라내는** 방식이라 대가가 지연 그 자체다.

반면 자체 폴러의 겹쳐 읽기는 **뒤로 물러나 읽되 앞으로는 끝까지 읽는다.**
늦게 커밋된 행을 잡으면서도 최신 행을 기다리지 않는다. 대가는 중복이고, 그건 멱등으로 거른다.

```
커넥터  : [마지막 읽은 시각 ... 현재시각 - delay]   ← 여기서 지연 발생
자체폴러: [커서 - 겹침폭 ... 현재시각]              ← 지연 없음, 중복은 멱등으로
```

지연 예산이 넉넉했다면 커넥터가 맞는 선택이었을 것이다. 코드를 한 줄도 안 써도 되고,
오프셋 관리와 재시작 복구를 프레임워크가 해준다.
**예산이 빡빡했기 때문에 직접 쓰는 쪽을 택했다.**

참고로 이 커넥터는 timestamp 모드에서 동일 타임스탬프 행이 배치 경계에 걸릴 때
나머지를 못 읽는 사례가 커뮤니티에 보고돼 있다. 위 설정은 그 완화책이다.

---

## 11. 남는 것

지연 예산을 최종적으로 비교하면 이렇게 됐다.

```
CDC                    캡처 잡 5.0초 + 커넥터 0.5초 + 나머지  ≈ 5.5초
JDBC 커넥터 (delay 5s) 위와 동일                              ≈ 5.5초
JDBC 커넥터 (delay 0)  약 1.5초. 단 놓친 건은 영구 손실
자체 폴러 + Kafka      폴링 1.0초 + Kafka 수 ms + 나머지     ≈ 1.3초
```

> ⚠️ **위 숫자는 벤더 문서의 기본값과 설정값으로 계산한 산정치이고 실측이 아니다.**
> 현재 설계를 확정하고 구현에 착수한 단계이며, 종단 지연 실측은 그 이후다.
> 특히 "나머지" 구간은 기존 배치 작업과의 스레드 경합에 따라 달라질 수 있어
> 평시가 아니라 **피크 동시 유입 + 모든 소비자 연결 상태**에서 다시 재야 한다.

Kafka를 하나 더 거치는데도 지연이 거의 안 늘어난다는 점이 흥미로웠다.
컨슈머의 fetch는 long poll이라 `fetch.min.bytes`가 1이면 데이터가 생기는 순간 깨어나기 때문이다.
**브로커에 요청을 걸어두면 데이터가 append되는 시점에 즉시 응답된다.**

이번에 얻은 것을 세 줄로 정리하면 이렇다.

**(1) 일반론은 DB 구현 앞에서 검증해야 한다.**
"CDC는 로그를 읽으므로 폴링이 아니다"는 MySQL과 PostgreSQL에서는 정확하지만
SQL Server에서는 절반만 맞다. 벤더 문서의 설정 기본값 한 줄이 설계 전체를 바꿨다.

**(2) 기술의 강점과 요구가 맞는지를 먼저 본다.**
CDC의 강점은 정확성인데 내가 급했던 건 지연과 착수 시점이었다.
좋은 기술이어도 **내 요구와 맞지 않으면 선택하지 않는 게 맞다.**

**(3) 층을 나누면 결정이 독립적으로 내려진다.**
"소비자가 10개로 는다"는 정보를 수집 층까지 끌고 가면 CDC를 다시 검토하게 된다.
배분 층의 문제로 한정하면 **Kafka는 넣고 CDC는 넣지 않는** 조합이 자연스럽게 나온다.

그리고 이 선택은 **되돌리기 싸다.**
이벤트 발행점을 한 곳으로 모아두면 나중에 수집 방식이 CDC로 바뀌든,
원본 시스템이 직접 이벤트를 밀어주는 형태로 바뀌든 그 아래는 하나도 안 바뀐다.

반대로 CDC를 먼저 택했다가 권한이 거부되면 그 사이의 시간은 회수되지 않는다.
**되돌리기 비싼 쪽을 나중에 두는 것**, 이번 검토에서 가장 실용적이었던 원칙이다.

---

## 참고

- [Debezium connector for SQL Server](https://debezium.io/documentation/reference/stable/connectors/sqlserver.html)
- [sys.sp_cdc_change_job (Transact-SQL)](https://learn.microsoft.com/en-us/sql/relational-databases/system-stored-procedures/sys-sp-cdc-change-job-transact-sql)
- [Aiven JDBC source connector configuration options](https://github.com/Aiven-Open/jdbc-connector-for-apache-kafka)
- [Apache Kafka Consumer Configs](https://kafka.apache.org/39/generated/consumer_config.html)
- [JDBC Source connector loses data in incrementing modes (Confluent Community)](https://forum.confluent.io/t/jdbc-source-connector-loses-data-in-incrementing-and-timestamp-incrementing-modes/3175)
