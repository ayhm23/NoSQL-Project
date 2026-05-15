# Pig + Hadoop Setup on Mac M2 (Apple Silicon)
### NASA Log ETL — DAS 839 Project

> **Estimated time:** ~30–45 minutes from scratch.  
> **Assumed:** Homebrew is installed, Java is installed.

---

## 0. Verify Java (Must Be ARM-Native Java 11)

Hadoop 3 + Pig 0.17.0 both require Java 8 or 11.  
On M2, always use an ARM-native JDK — never Rosetta x86_64 builds.

```bash
# Check current Java
java -version
# Must show: openjdk version "11.x.x" (not a Rosetta/x86_64 build)

# Install ARM-native Java 11 via Homebrew (if not already)
brew install openjdk@11

# Add to your shell profile (~/.zshrc or ~/.bash_profile)
echo 'export JAVA_HOME=/opt/homebrew/opt/openjdk@11' >> ~/.zshrc
echo 'export PATH="$JAVA_HOME/bin:$PATH"' >> ~/.zshrc
source ~/.zshrc

# Verify
java -version
# Should show: openjdk version "11.x.x" 2021-...
```

---

## 1. Install Hadoop via Homebrew

```bash
brew install hadoop
```

Homebrew installs Hadoop 3.3.x to:
- **HADOOP_HOME:** `/opt/homebrew/opt/hadoop/libexec`
- **Config files:** `/opt/homebrew/opt/hadoop/libexec/etc/hadoop/`
- **Binaries:** `/opt/homebrew/opt/hadoop/bin/` (symlinked to `/opt/homebrew/bin/`)

Add Hadoop to your shell profile:

```bash
echo 'export HADOOP_HOME=/opt/homebrew/opt/hadoop/libexec' >> ~/.zshrc
echo 'export PATH="$HADOOP_HOME/bin:$HADOOP_HOME/sbin:$PATH"' >> ~/.zshrc
source ~/.zshrc

# Quick check
hadoop version
# Should print: Hadoop 3.3.x
```

---

## 2. Configure Hadoop (Pseudo-Distributed Mode)

All config files are in `/opt/homebrew/opt/hadoop/libexec/etc/hadoop/`.

### 2.1 `hadoop-env.sh` — Set JAVA_HOME

```bash
# Open the file
nano /opt/homebrew/opt/hadoop/libexec/etc/hadoop/hadoop-env.sh
```

Find the `JAVA_HOME` line and set it:

```bash
export JAVA_HOME=/opt/homebrew/opt/openjdk@11
```

### 2.2 `core-site.xml` — Point to local HDFS

Replace the content of the `<configuration>` block:

```xml
<configuration>
  <property>
    <name>fs.defaultFS</name>
    <value>hdfs://localhost:9000</value>
  </property>
  <property>
    <name>hadoop.tmp.dir</name>
    <value>/tmp/hadoop-${user.name}</value>
  </property>
</configuration>
```

### 2.3 `hdfs-site.xml` — Single-node replication

```xml
<configuration>
  <property>
    <name>dfs.replication</name>
    <value>1</value>
  </property>
  <property>
    <name>dfs.namenode.name.dir</name>
    <value>/tmp/hadoop-${user.name}/dfs/name</value>
  </property>
  <property>
    <name>dfs.datanode.data.dir</name>
    <value>/tmp/hadoop-${user.name}/dfs/data</value>
  </property>
</configuration>
```

### 2.4 `mapred-site.xml` — Use YARN as scheduler

```xml
<configuration>
  <property>
    <name>mapreduce.framework.name</name>
    <value>yarn</value>
  </property>
  <property>
    <name>mapreduce.application.classpath</name>
    <value>$HADOOP_HOME/share/hadoop/mapreduce/*:$HADOOP_HOME/share/hadoop/mapreduce/lib/*</value>
  </property>
</configuration>
```

### 2.5 `yarn-site.xml` — Resource manager settings

```xml
<configuration>
  <property>
    <name>yarn.nodemanager.aux-services</name>
    <value>mapreduce_shuffle</value>
  </property>
  <property>
    <name>yarn.nodemanager.env-whitelist</name>
    <value>JAVA_HOME,HADOOP_COMMON_HOME,HADOOP_HDFS_HOME,HADOOP_CONF_DIR,CLASSPATH_PREPEND_DISTCACHE,HADOOP_YARN_HOME,HADOOP_HOME,PATH,LANG,TZ,HADOOP_MAPRED_HOME</value>
  </property>
</configuration>
```

---

## 3. Enable SSH to Localhost (Required by Hadoop)

Hadoop uses SSH to manage daemons. On Mac you need two things:

### 3.1 Enable Remote Login in System Settings

```
System Settings → General → Sharing → Remote Login → Enable
```

Grant access to your current user.

### 3.2 Set Up Passwordless SSH Keys

```bash
# Generate key (skip if ~/.ssh/id_rsa already exists)
ssh-keygen -t rsa -P '' -f ~/.ssh/id_rsa

# Authorize it for localhost
cat ~/.ssh/id_rsa.pub >> ~/.ssh/authorized_keys
chmod 600 ~/.ssh/authorized_keys

# Test — must connect WITHOUT asking for a password
ssh localhost
# If prompted "Are you sure...?" type yes, then check you got a shell
exit
```

---

## 4. Format the HDFS Namenode (One-Time Only)

> **⚠️ Only run this once.** Running it again wipes all HDFS data.

```bash
hdfs namenode -format
# You will see a prompt: "Re-format filesystem... (Y or N)?"
# Type Y if first time, or if you want a clean slate.
```

---

## 5. Start Hadoop Services

```bash
# Start HDFS (NameNode + DataNode)
start-dfs.sh

# Start YARN (ResourceManager + NodeManager)
start-yarn.sh

# Verify all 4 daemons are running
jps
# Expected output (order may vary):
# 12345 NameNode
# 12346 DataNode
# 12347 SecondaryNameNode
# 12348 ResourceManager
# 12349 NodeManager
```

**Web UIs (useful for monitoring):**
- HDFS NameNode: http://localhost:9870
- YARN ResourceManager: http://localhost:8088

---

## 6. Install Apache Pig 0.17.0

Pig is not available via Homebrew. Download the binary release manually.

```bash
# Download Pig 0.17.0 binary
cd ~
curl -O https://downloads.apache.org/pig/pig-0.17.0/pig-0.17.0.tar.gz

# Extract
tar -xzf pig-0.17.0.tar.gz
mv pig-0.17.0 ~/pig-0.17.0

# Add to shell profile
echo 'export PIG_HOME=$HOME/pig-0.17.0' >> ~/.zshrc
echo 'export PATH="$PIG_HOME/bin:$PATH"' >> ~/.zshrc
source ~/.zshrc

# Verify
pig --version
# Expected: Apache Pig version 0.17.0
```

### 6.1 Fix Pig–Hadoop 3 Classpath Compatibility

Pig 0.17.0 was released for Hadoop 2.x. To make it work with Hadoop 3, you must
set `PIG_CLASSPATH` to include all Hadoop 3 jars. Add this to `~/.zshrc`:

```bash
export PIG_CLASSPATH=\
$HADOOP_HOME/share/hadoop/common/*:\
$HADOOP_HOME/share/hadoop/common/lib/*:\
$HADOOP_HOME/share/hadoop/hdfs/*:\
$HADOOP_HOME/share/hadoop/hdfs/lib/*:\
$HADOOP_HOME/share/hadoop/mapreduce/*:\
$HADOOP_HOME/share/hadoop/mapreduce/lib/*:\
$HADOOP_HOME/share/hadoop/yarn/*:\
$HADOOP_HOME/share/hadoop/yarn/lib/*:\
$HADOOP_HOME/etc/hadoop
```

Also add this to `$PIG_HOME/conf/pig.properties` (create if missing):

```bash
mkdir -p ~/pig-0.17.0/conf
cat >> ~/pig-0.17.0/conf/pig.properties << 'EOF'
pig.use.overriden.hadoop.configs=true
EOF
```

Reload:
```bash
source ~/.zshrc
```

### 6.2 Quick Smoke Test

```bash
# Test Pig can see Hadoop (should print no errors)
pig -x mapreduce -e "A = LOAD '/'; DUMP A;"
# Expected: returns quickly (empty HDFS root), no ClassNotFoundError
```

---

## 7. Full Environment Summary

After all steps, your `~/.zshrc` should contain these blocks:

```bash
# Java
export JAVA_HOME=/opt/homebrew/opt/openjdk@11
export PATH="$JAVA_HOME/bin:$PATH"

# Hadoop
export HADOOP_HOME=/opt/homebrew/opt/hadoop/libexec
export PATH="$HADOOP_HOME/bin:$HADOOP_HOME/sbin:$PATH"

# Pig
export PIG_HOME=$HOME/pig-0.17.0
export PATH="$PIG_HOME/bin:$PATH"

# Pig–Hadoop 3 classpath fix
export PIG_CLASSPATH=\
$HADOOP_HOME/share/hadoop/common/*:\
$HADOOP_HOME/share/hadoop/common/lib/*:\
$HADOOP_HOME/share/hadoop/hdfs/*:\
$HADOOP_HOME/share/hadoop/hdfs/lib/*:\
$HADOOP_HOME/share/hadoop/mapreduce/*:\
$HADOOP_HOME/share/hadoop/mapreduce/lib/*:\
$HADOOP_HOME/share/hadoop/yarn/*:\
$HADOOP_HOME/share/hadoop/yarn/lib/*:\
$HADOOP_HOME/etc/hadoop
```

---

## 8. Project-Level Config Updates

### 8.1 Add to `config.py`

```python
# ─────────────────────────────────────────────
# Pig / Hadoop
# ─────────────────────────────────────────────
HADOOP_HOME     = os.getenv("HADOOP_HOME", "/opt/homebrew/opt/hadoop/libexec")
PIG_HOME        = os.getenv("PIG_HOME",    str(Path.home() / "pig-0.17.0"))
PIG_SCRIPTS_DIR = str(BASE_DIR / "pig")          # where .pig files live
PIG_HDFS_BASE   = os.getenv("PIG_HDFS_BASE", "/nasa_etl")  # HDFS working root
```

### 8.2 Update `main.py` — Register the Pig Pipeline

In `_get_pipeline_class()`, replace the stub:

```python
# BEFORE:
elif name == "pig":
    raise NotImplementedError("Pig pipeline not yet implemented")

# AFTER:
elif name == "pig":
    from pipelines.pig_pipeline import PigPipeline
    return PigPipeline
```

---

## 9. Project Directory After Adding Pig

```
NoSQL-Project/
├── pig/                         ← NEW: Pig Latin scripts
│   ├── q1_daily_traffic.pig
│   ├── q2_top_resources.pig
│   └── q3_hourly_errors.pig
├── pipelines/
│   ├── base_pipeline.py
│   ├── mongo_pipeline.py
│   ├── mapreduce_pipeline.py
│   └── pig_pipeline.py          ← NEW
└── ...
```

---

## 10. Running the Pig Pipeline

Make sure Hadoop is running (`jps` shows 5 daemons), then:

```bash
# Basic run
python main.py --pipeline pig --batch-size 50000

# Run and generate comparison report immediately after
python main.py --pipeline pig --batch-size 50000 --report

# View just the report from a previous run
python main.py --pipeline pig --report-only
```

---

## 11. Stopping Hadoop

```bash
stop-yarn.sh
stop-dfs.sh
```

---

## 12. Troubleshooting

| Symptom | Fix |
|---|---|
| `Connection refused` on HDFS | Run `start-dfs.sh`; check `jps` |
| `ssh: connect to host localhost port 22` | Enable Remote Login in System Settings |
| `ClassNotFoundException` in Pig | Check `PIG_CLASSPATH` includes all Hadoop 3 jars |
| `SafeModeException` on HDFS | Wait 30s after `start-dfs.sh`; or run `hdfs dfsadmin -safemode leave` |
| `Permission denied` on SSH | Run `chmod 600 ~/.ssh/authorized_keys` |
| Pig hangs at `[main] INFO ...HadoopShims` | YARN is not running; run `start-yarn.sh` |
| Namenode won't start | Check `/tmp/hadoop-<user>/dfs/name` exists and is writable |
