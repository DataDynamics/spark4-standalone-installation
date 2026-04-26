# Apache Spark 4.1.1 Standalone Cluster 설치 가이드 (RHEL 9, Airgap)

본 문서는 Mesos / YARN / Kubernetes 없이 Apache Spark 4.1.1 의 **Standalone 클러스터 모드** 를 RHEL 9 기반의 3 대 서버에 구성하고, **PySpark** 를 사용하기 위한 설치 과정을 정리한 문서입니다. 모든 노드는 **인터넷이 차단된 Airgap 환경** 임을 전제로 합니다.

## 1. 클러스터 구성

| Hostname | IP            | 역할                |
| -------- | ------------- | ------------------- |
| spark01  | 192.168.1.1   | Master + Worker     |
| spark02  | 192.168.1.2   | Worker              |
| spark03  | 192.168.1.3   | Worker              |

- OS: Red Hat Enterprise Linux 9 (x86_64)
- Spark: 4.1.1 (Hadoop 3 / Scala 2.13 사전 빌드 바이너리)
- Java: OpenJDK 17 (Spark 4.x 의 최소 요구사항)
- Python: 3.11 이상 (PySpark 4.x 권장)
- 설치 경로: `/opt/spark`
- 실행 계정: `spark` (전용 시스템 사용자)

> Standalone 모드에서 Master 와 Worker 는 같은 노드에서 동시에 실행 가능합니다. 본 가이드에서는 192.168.1.1 에서 Master 데몬과 Worker 데몬을 함께 기동합니다.

---

## 2. 사전 준비 — Airgap 환경에서 다운로드할 파일

인터넷이 가능한 Staging 머신에서 아래 파일을 미리 받아 두고 USB / 내부 파일서버 등을 통해 3 대의 노드로 옮깁니다.

### 2.1 필수 파일

1. **Spark 4.1.1 바이너리 (Hadoop 3, Scala 2.13)**
   - 파일명 예: `spark-4.1.1-bin-hadoop3-scala2.13.tgz`
   - 다운로드: <https://archive.apache.org/dist/spark/spark-4.1.1/>

2. **OpenJDK 17 RPM** (RHEL 9 서브스크립션이 없는 경우)
   - 파일명 예: `java-17-openjdk-headless-17.0.x.x-x.el9.x86_64.rpm`
   - Red Hat 고객 포털 또는 미러된 yum 저장소에서 RPM 다운로드

3. **Python 3.11 RPM** (RHEL 9 기본 저장소에 포함)
   - `python3.11`, `python3.11-pip` 패키지 RPM
   - PySpark 외부 라이브러리가 필요한 경우 wheel 파일을 추가로 다운로드

4. **PySpark 4.1.1 wheel** (선택, Driver 머신에서 `pip install pyspark` 가 필요할 때)
   - `pip download pyspark==4.1.1 -d ./pyspark-offline`

### 2.2 디렉터리 구조 예시

Staging 머신에서 다음과 같이 묶어 두면 배포가 편리합니다.

```
spark-airgap-bundle/
├── spark-4.1.1-bin-hadoop3-scala2.13.tgz
├── rpms/
│   ├── java-17-openjdk-headless-*.rpm
│   ├── java-17-openjdk-*.rpm
│   ├── python3.11-*.rpm
│   └── python3.11-pip-*.rpm
└── pyspark-offline/
    └── *.whl
```

번들을 3 대 노드의 `/tmp/spark-airgap-bundle/` 로 복사합니다.

---

## 3. 모든 노드 공통 작업

> 별도 표기가 없으면 192.168.1.1, 192.168.1.2, 192.168.1.3 **세 노드 모두** 에서 동일하게 수행합니다.

### 3.1 호스트 이름 / hosts 등록

```bash
# spark01 노드에서
sudo hostnamectl set-hostname spark01
# spark02 노드에서
sudo hostnamectl set-hostname spark02
# spark03 노드에서
sudo hostnamectl set-hostname spark03
```

세 노드 모두 `/etc/hosts` 에 다음을 추가합니다.

```
192.168.1.1  spark01
192.168.1.2  spark02
192.168.1.3  spark03
```

### 3.2 방화벽 / SELinux

Spark Standalone 통신에 필요한 포트를 열거나 내부망 한정으로 firewalld 를 비활성화합니다.

| 포트  | 용도                          | 노드        |
| ----- | ----------------------------- | ----------- |
| 7077  | Master RPC                    | Master      |
| 8080  | Master Web UI                 | Master      |
| 8081  | Worker Web UI                 | Worker      |
| 4040  | Driver/Application Web UI     | Driver      |
| 7337  | Worker → Master block manager | Worker      |
| 18080 | History Server (선택)         | History 노드 |

```bash
sudo firewall-cmd --permanent --add-port=7077/tcp
sudo firewall-cmd --permanent --add-port=8080/tcp
sudo firewall-cmd --permanent --add-port=8081/tcp
sudo firewall-cmd --permanent --add-port=4040/tcp
sudo firewall-cmd --permanent --add-port=7337/tcp
sudo firewall-cmd --reload
```

SELinux 는 그대로 `enforcing` 으로 두어도 표준 경로(`/opt/spark`) 사용 시 문제가 없습니다.

### 3.3 OpenJDK 17 설치 (Airgap RPM 사용)

```bash
cd /tmp/spark-airgap-bundle/rpms
sudo dnf install -y --disablerepo='*' ./java-17-openjdk-headless-*.rpm ./java-17-openjdk-*.rpm
java -version
```

`/etc/profile.d/java.sh` 를 생성합니다.

```bash
sudo tee /etc/profile.d/java.sh >/dev/null <<'EOF'
export JAVA_HOME=/usr/lib/jvm/java-17-openjdk
export PATH=$JAVA_HOME/bin:$PATH
EOF
sudo chmod 644 /etc/profile.d/java.sh
source /etc/profile.d/java.sh
```

### 3.4 Python 3.11 설치

```bash
cd /tmp/spark-airgap-bundle/rpms
sudo dnf install -y --disablerepo='*' ./python3.11-*.rpm ./python3.11-pip-*.rpm
python3.11 --version
```

### 3.5 spark 사용자 생성

```bash
sudo useradd --system --create-home --home-dir /home/spark --shell /bin/bash spark
sudo mkdir -p /opt/spark /var/log/spark /var/run/spark
sudo chown -R spark:spark /opt/spark /var/log/spark /var/run/spark
```

### 3.6 Spark 바이너리 설치

```bash
sudo -u spark tar -xzf /tmp/spark-airgap-bundle/spark-4.1.1-bin-hadoop3-scala2.13.tgz -C /opt
sudo -u spark ln -s /opt/spark-4.1.1-bin-hadoop3-scala2.13 /opt/spark
ls -l /opt/spark/
```

### 3.7 환경 변수

`/etc/profile.d/spark.sh` 를 생성합니다.

```bash
sudo tee /etc/profile.d/spark.sh >/dev/null <<'EOF'
export SPARK_HOME=/opt/spark
export PATH=$SPARK_HOME/bin:$SPARK_HOME/sbin:$PATH
export PYSPARK_PYTHON=/usr/bin/python3.11
export PYSPARK_DRIVER_PYTHON=/usr/bin/python3.11
EOF
sudo chmod 644 /etc/profile.d/spark.sh
source /etc/profile.d/spark.sh
```

---

## 4. SSH 무인 접속 (Master → Worker)

Master 노드에서 `start-workers.sh` 를 사용해 Worker 들을 한 번에 기동하려면, Master 의 `spark` 계정이 모든 Worker 의 `spark` 계정으로 비밀번호 없이 SSH 접속이 가능해야 합니다. (단일 노드 별로 직접 데몬을 띄울 거라면 이 단계는 건너뛰어도 됩니다.)

### 4.1 Master 노드 (192.168.1.1)

```bash
sudo -iu spark
ssh-keygen -t ed25519 -N '' -f ~/.ssh/id_ed25519
```

### 4.2 공개키 배포

Airgap 환경이므로 `ssh-copy-id` 가 가능한지 먼저 확인하고, 불가능하면 USB 등으로 `~/.ssh/id_ed25519.pub` 를 옮겨 각 Worker 노드의 `/home/spark/.ssh/authorized_keys` 에 추가합니다.

```bash
# 각 Worker 노드 (spark 계정으로)
mkdir -p ~/.ssh && chmod 700 ~/.ssh
cat /tmp/id_ed25519.pub >> ~/.ssh/authorized_keys
chmod 600 ~/.ssh/authorized_keys
```

Master 에서 동작 확인:

```bash
ssh spark@spark01 'hostname'
ssh spark@spark02 'hostname'
ssh spark@spark03 'hostname'
```

---

## 5. Spark 설정 파일 (모든 노드 동일)

### 5.1 `$SPARK_HOME/conf/spark-env.sh`

```bash
sudo -u spark cp /opt/spark/conf/spark-env.sh.template /opt/spark/conf/spark-env.sh
sudo -u spark tee /opt/spark/conf/spark-env.sh >/dev/null <<'EOF'
#!/usr/bin/env bash
export JAVA_HOME=/usr/lib/jvm/java-17-openjdk

# Master
export SPARK_MASTER_HOST=spark01
export SPARK_MASTER_PORT=7077
export SPARK_MASTER_WEBUI_PORT=8080

# Worker (서버 사양에 맞게 조정)
export SPARK_WORKER_CORES=8
export SPARK_WORKER_MEMORY=16g
export SPARK_WORKER_PORT=7078
export SPARK_WORKER_WEBUI_PORT=8081

# 로그 / 작업 디렉터리
export SPARK_LOG_DIR=/var/log/spark
export SPARK_PID_DIR=/var/run/spark
export SPARK_WORKER_DIR=/var/lib/spark/work

# PySpark 가 사용할 Python 인터프리터
export PYSPARK_PYTHON=/usr/bin/python3.11
export PYSPARK_DRIVER_PYTHON=/usr/bin/python3.11
EOF
sudo chmod 755 /opt/spark/conf/spark-env.sh
sudo mkdir -p /var/lib/spark/work && sudo chown spark:spark /var/lib/spark/work
```

### 5.2 `$SPARK_HOME/conf/spark-defaults.conf`

```bash
sudo -u spark tee /opt/spark/conf/spark-defaults.conf >/dev/null <<'EOF'
spark.master                     spark://spark01:7077
spark.serializer                 org.apache.spark.serializer.KryoSerializer
spark.eventLog.enabled           true
spark.eventLog.dir               file:/var/log/spark/events
spark.history.fs.logDirectory    file:/var/log/spark/events
EOF
sudo -u spark mkdir -p /var/log/spark/events
```

### 5.3 `$SPARK_HOME/conf/workers`

Master 에서 `start-workers.sh` 로 일괄 기동할 때 참조됩니다. **Master 노드에만** 작성하면 충분합니다.

```bash
sudo -u spark tee /opt/spark/conf/workers >/dev/null <<'EOF'
spark01
spark02
spark03
EOF
```

### 5.4 (선택) 로깅 레벨 조정

```bash
sudo -u spark cp /opt/spark/conf/log4j2.properties.template /opt/spark/conf/log4j2.properties
```

---

## 6. 클러스터 기동 / 종료

### 6.1 일괄 기동 (Master 192.168.1.1 에서)

```bash
sudo -iu spark
$SPARK_HOME/sbin/start-master.sh
$SPARK_HOME/sbin/start-workers.sh   # workers 파일에 정의된 모든 노드 기동
```

### 6.2 노드별 수동 기동 (SSH 무인 접속을 설정하지 않았을 때)

```bash
# Master (192.168.1.1)
sudo -iu spark
$SPARK_HOME/sbin/start-master.sh

# 각 Worker (192.168.1.1, 192.168.1.2, 192.168.1.3)
sudo -iu spark
$SPARK_HOME/sbin/start-worker.sh spark://spark01:7077
```

### 6.3 종료

```bash
# Master 노드에서
$SPARK_HOME/sbin/stop-workers.sh
$SPARK_HOME/sbin/stop-master.sh
```

### 6.4 상태 확인

- Master Web UI: <http://192.168.1.1:8080> — 3 개의 Worker 가 `ALIVE` 로 등록되어야 합니다.
- Worker 로그: `/var/log/spark/spark-spark-org.apache.spark.deploy.worker.Worker-*.out`
- Master 로그: `/var/log/spark/spark-spark-org.apache.spark.deploy.master.Master-*.out`

---

## 7. (선택) systemd 서비스 등록

재부팅 후 자동 기동을 위해 systemd 유닛을 작성할 수 있습니다.

### 7.1 Master — `/etc/systemd/system/spark-master.service` (192.168.1.1)

```ini
[Unit]
Description=Apache Spark Master
After=network-online.target
Wants=network-online.target

[Service]
Type=forking
User=spark
Group=spark
EnvironmentFile=/etc/profile.d/java.sh
EnvironmentFile=/etc/profile.d/spark.sh
ExecStart=/opt/spark/sbin/start-master.sh
ExecStop=/opt/spark/sbin/stop-master.sh
Restart=on-failure

[Install]
WantedBy=multi-user.target
```

### 7.2 Worker — `/etc/systemd/system/spark-worker.service` (모든 노드)

```ini
[Unit]
Description=Apache Spark Worker
After=network-online.target
Wants=network-online.target

[Service]
Type=forking
User=spark
Group=spark
EnvironmentFile=/etc/profile.d/java.sh
EnvironmentFile=/etc/profile.d/spark.sh
ExecStart=/opt/spark/sbin/start-worker.sh spark://spark01:7077
ExecStop=/opt/spark/sbin/stop-worker.sh
Restart=on-failure

[Install]
WantedBy=multi-user.target
```

> systemd 는 `EnvironmentFile` 로 `export` 키워드를 해석하지 못하므로, 필요한 경우 `JAVA_HOME=...` / `SPARK_HOME=...` 형태로 별도 환경 파일을 작성하거나 `Environment=` 지시자를 사용해 직접 지정합니다.

```bash
sudo systemctl daemon-reload
sudo systemctl enable --now spark-master    # 192.168.1.1 만
sudo systemctl enable --now spark-worker    # 세 노드 모두
```

---

## 8. PySpark 사용

### 8.1 `pyspark` 셸 (Master 노드에서)

```bash
sudo -iu spark
pyspark --master spark://spark01:7077
```

```python
>>> sc.parallelize(range(1, 1001)).sum()
500500
>>> spark.range(10).show()
```

### 8.2 `spark-submit` 으로 Python 작업 제출

`/home/spark/wordcount.py` 예시:

```python
from pyspark.sql import SparkSession

spark = (SparkSession.builder
         .appName("wordcount")
         .getOrCreate())

text = spark.sparkContext.parallelize(
    ["hello spark", "hello pyspark", "spark on standalone"]
)
counts = (text.flatMap(lambda l: l.split())
              .map(lambda w: (w, 1))
              .reduceByKey(lambda a, b: a + b)
              .collect())

for word, count in counts:
    print(f"{word}\t{count}")

spark.stop()
```

```bash
spark-submit --master spark://spark01:7077 \
             --deploy-mode client \
             /home/spark/wordcount.py
```

### 8.3 외부 클라이언트에서 PySpark 사용 (Airgap 오프라인 설치)

Driver 머신에 `pyspark` 패키지를 설치하려면, 미리 받아둔 wheel 들로 오프라인 설치합니다.

```bash
python3.11 -m venv ~/venv-pyspark
source ~/venv-pyspark/bin/activate
pip install --no-index --find-links=/tmp/spark-airgap-bundle/pyspark-offline pyspark==4.1.1
```

이 후 Python 코드에서 다음과 같이 클러스터에 연결합니다.

```python
from pyspark.sql import SparkSession

spark = (SparkSession.builder
         .master("spark://192.168.1.1:7077")
         .appName("client-app")
         .getOrCreate())
```

> Driver 와 Executor 의 Python 버전은 **반드시 동일** 해야 합니다. 모든 노드에 동일한 `python3.11` 이 설치되어 있어야 합니다.

---

## 9. 점검 체크리스트

- [ ] 세 노드에서 `java -version` 이 OpenJDK 17 을 출력하는가
- [ ] 세 노드에서 `python3.11 --version` 이 정상인가
- [ ] 세 노드의 `/etc/hosts` 에 spark01 / spark02 / spark03 가 등록되었는가
- [ ] 방화벽에 7077 / 8080 / 8081 / 4040 / 7337 포트가 허용되었는가
- [ ] Master Web UI(<http://192.168.1.1:8080>)에 Worker 3 개가 모두 ALIVE 인가
- [ ] `spark-submit --master spark://spark01:7077 ...` 로 샘플 잡이 성공하는가

---

## 10. 트러블슈팅

| 증상                                              | 원인 / 조치                                                                 |
| ------------------------------------------------- | --------------------------------------------------------------------------- |
| Worker 가 Master 에 등록되지 않음                  | `/etc/hosts` 의 hostname → IP 매핑, 방화벽 7077 포트, `SPARK_MASTER_HOST` 확인 |
| `java.lang.UnsupportedClassVersionError`          | JDK 버전 불일치. Spark 4.x 는 JDK 17 이상 필요                              |
| `Python in worker has different version than ...` | 노드별 Python 버전 상이. 모든 노드에서 `PYSPARK_PYTHON` 을 동일 버전으로 통일 |
| `Permission denied` (SSH)                          | `spark` 계정의 `~/.ssh/authorized_keys` 권한(600), `.ssh` 디렉터리 권한(700) 확인 |
| Worker 로그에 `Cannot allocate memory`            | `SPARK_WORKER_MEMORY`, `SPARK_WORKER_CORES` 를 서버 사양에 맞춰 축소         |

---

## 부록 A. 디렉터리 요약

| 경로                       | 용도                       |
| -------------------------- | -------------------------- |
| `/opt/spark`               | Spark 설치 디렉터리 (심볼릭 링크) |
| `/opt/spark/conf`          | 설정 파일                  |
| `/var/log/spark`           | 데몬 로그 / 이벤트 로그    |
| `/var/run/spark`           | PID 파일                   |
| `/var/lib/spark/work`      | Worker 작업 디렉터리       |
| `/home/spark`              | spark 계정 홈              |
