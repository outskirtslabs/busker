(ns tasks.hcloud
  (:require
   [babashka.cli :as cli]
   [babashka.process :as p]
   [clojure.string :as str]))

(def ^:private server-name "busker-bench")
(def ^:private remote-directory "/busker")
(def ^:private hcloud-command ["hcloud" "--context" "busker"])

(defn- hcloud!
  [& args]
  (apply p/shell (into hcloud-command args)))

(defn- server-ip []
  (-> (apply p/shell {:out :string}
             (into hcloud-command ["server" "ip" server-name]))
      :out
      str/trim))

(defn- ssh!
  [command]
  (hcloud! "server" "ssh" server-name
           "-o" "StrictHostKeyChecking=accept-new"
           command))

(defn create
  "Creates the Busker benchmark server."
  [_]
  (hcloud! "server" "create"
           "--name" server-name
           "--ssh-key" "casey"
           "--type" "ccx13"
           "--image" "ubuntu-26.04"))

(defn setup
  "Installs the Busker benchmark prerequisites on the server."
  [_]
  (ssh! (str "export DEBIAN_FRONTEND=noninteractive &&"
             " apt-get update &&"
             " if ! apt-cache show openjdk-25-jdk-headless >/dev/null 2>&1;"
             " then echo 'openjdk-25-jdk-headless is not available from apt' >&2;"
             " exit 1; fi &&"
             " apt-get install -y --no-install-recommends"
             " ca-certificates curl git openssh-client"
             " openjdk-25-jdk-headless rlwrap rsync wrk &&"
             " update-alternatives --set java"
             " /usr/lib/jvm/java-25-openjdk-amd64/bin/java &&"
             " if ! [ -x /usr/local/bin/clojure ] ||"
             " ! /usr/local/bin/clojure -Sdescribe >/dev/null 2>&1; then"
             " tmp=$(mktemp -d) &&"
             " curl -fsSL -o $tmp/linux-install.sh"
             " https://github.com/clojure/brew-install/"
             "releases/latest/download/linux-install.sh &&"
             " bash $tmp/linux-install.sh && rm -rf $tmp; fi &&"
             " java_spec=$(java -XshowSettings:properties -version 2>&1 |"
             " awk -F'= ' '/java.specification.version/ {print $2; exit}') &&"
             " case $java_spec in 2[5-9]|[3-9][0-9]*) ;;"
             " *) echo \"Java 25 or newer is required, found $java_spec\" >&2;"
             " exit 1 ;; esac")))

(defn upload
  "Uploads the current directory to the Busker benchmark server."
  [_]
  (let [ip (server-ip)
        target (str "root@" ip ":" remote-directory)]
    (println "Uploading current directory to benchmarking server...")
    (p/shell "rsync" "-az"
             "--exclude=.worktrees/"
             "--exclude=shim/"
             "--exclude=.lsp/"
             "--exclude=archive/"
             "."
             target)
    (p/shell "rsync" "-azR"
             "shim/linux-aarch64/deps.edn"
             "shim/linux-x86-64/deps.edn"
             "shim/linux-x86-64/resources/"
             "shim/macos-aarch64/deps.edn"
             "shim/macos-x86-64/deps.edn"
             target)))

(defn bench
  "Uploads Busker and runs its Ring adapter benchmarks on the server."
  [opts]
  (upload opts)
  (ssh! (str "cd " remote-directory
             " && /usr/local/bin/clojure -X:deps prep :aliases '[:bench]'"
             " && timeout --signal=TERM --kill-after=10s 30m"
             " /usr/local/bin/clojure -M:bench")))

(defn delete
  "Deletes the Busker benchmark server."
  [_]
  (hcloud! "server" "delete" server-name))

(defn- report-error
  [{:keys [msg wrong-input]}]
  (throw (ex-info (or msg (str "Unknown hcloud command: " wrong-input)) {})))

(defn run
  "Runs a Hetzner Cloud benchmark-server command."
  [& args]
  (if (or (empty? args)
          (some #{"-h" "--help"} args))
    (println (str "Usage: bb hcloud <command>\n\n"
                  "Commands:\n"
                  "  create  Create the benchmark server\n"
                  "  setup   Install benchmark prerequisites\n"
                  "  upload  Upload the current directory\n"
                  "  bench   Upload and run the benchmarks\n"
                  "  delete  Delete the benchmark server"))
    (cli/dispatch [{:cmds ["create"] :fn create :restrict true}
                   {:cmds ["setup"] :fn setup :restrict true}
                   {:cmds ["upload"] :fn upload :restrict true}
                   {:cmds ["bench"] :fn bench :restrict true}
                   {:cmds ["delete"] :fn delete :restrict true}]
                  args
                  {:error-fn report-error})))
