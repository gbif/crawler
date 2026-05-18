# GBIF Crawler

This project is responsible for coordinating dataset crawling. The coordinator, crawler, and CLI modules work together
to do the actual crawling.

The webservice and webservice client present crawling status as recorded in Zookeeper.

The Crawler project includes:
  1. [crawler](crawler/README.md): Contains the actual crawlers that speak the various XML and DWC-A/ABCD-A/CamtrapDP dialects needed for crawling the GBIF network
  2. [crawler-cleanup](crawler-cleanup/README.md): Used to delete crawl jobs in Zookeeper (see sub-module README for details how to use)
  3. [crawler-cli](crawler-cli/README.md): Provides the services that listen to RabbitMQ for instructions to crawl resources
  4. [crawler-coordinator](crawler-coordinator/README.md): Coordinates crawling jobs via Zookeeper (Curator)
  5. [crawler-ws](crawler-ws/README.md): Exposes read only crawl status and access to logs.
  6. [crawler-ws-client](crawler-ws-client/README.md): Java client to the WS

## Building
See the individual sub-module READMEs for specific details, but in general it is enough to build all components with:

````shell
mvn clean package
````

## Docker and Kubernetes deployment

The crawler CLI ships as a single container image built from the root `Dockerfile`. The image contains the
`crawler-cli` fat jar and runs any CLI command by setting `CRAWLER_COMMAND` at runtime. The container entrypoint
([`scripts/entrypoint-crawler-cli.sh`](scripts/entrypoint-crawler-cli.sh)) materialises a YAML config from environment
variables and Helm-injected values before invoking `java -jar /app/crawler-cli.jar "$CRAWLER_COMMAND" --conf ...`.

Kubernetes deployment is managed through the Helm chart in [`helm/`](helm/). The chart is intentionally generic: each
CLI command is configured under `commands` in [`helm/values.yaml`](helm/values.yaml). Long-running listeners such as
downloaders and metasync services are deployed as `Deployment` resources, while one-off commands such as `startcrawl`,
`crawleverything`, `metasynceverything`, and `pusheml` can be deployed as `Job` resources when enabled.

RabbitMQ and Registry credentials should normally be provided as existing Kubernetes Secrets via
`rabbit.credentialsSecret` and `registry.credentialsSecret`. The chart can create development secrets when
`createSecret` is enabled, but production deployments should avoid storing real credentials in values files.

Archive storage must be shared by services that hand archives off to one another. In normal environments this should be
an NFS or other shared volume configured in `helm/values.yaml` under `volumes`. The default `emptyDir` values are only
suitable for local rendering or isolated development because each pod receives its own temporary filesystem.

### Local dependencies for development

For running the CLI locally against a real RabbitMQ and ZooKeeper without Kubernetes, bring up the lightweight stack in
[`docker-compose.local.yml`](docker-compose.local.yml):

````shell
task local           # docker compose -f docker-compose.local.yml up -d
````

Both services include healthchecks and listen on their default ports (5672 / 15672 / 2181). The CLI itself is intended
to run from your IDE/terminal in this mode.

To run the packaged image instead of running the CLI from your IDE, `Taskfile.dist.yml` provides a generic
`docker:run:local` task that auto-creates the archive directory under `~/data` and wires the container against
`host.docker.internal` (works on Docker Desktop and on Linux thanks to `--add-host=host.docker.internal:host-gateway`):

````shell
task docker:run:local                                                         # dwcdp-metasync, ~/data/dwcdp
DATA_DIR=coldp CRAWLER_COMMAND=coldp-metasync   task docker:run:local         # coldp-metasync, ~/data/coldp
DATA_DIR=dwca  CRAWLER_COMMAND=downloader       task docker:run:local
DATA_DIR=abcda CRAWLER_COMMAND=abcdadownloader  task docker:run:local
````

`DATA_DIR` should match the `archiveRepository` name in [`helm/values.yaml`](helm/values.yaml). Commands that require
multiple shared volumes or extra repository paths (`dwcdpdownloader`, `validator`, `dwca-metasync`, `camtrapdp*`) are
not covered by `docker:run:local`; for those, call `docker:run` directly with the appropriate mounts/config, or run the
CLI from your IDE against the local stack.

### Building and pushing the image (multi-arch)

GBIF cluster nodes run on `linux/amd64`, but Apple Silicon developer machines build `linux/arm64` by default. A
single-architecture push will fail at runtime with `exec format error` on the cluster. Use the multi-arch task helper
in [`Taskfile.dist.yml`](Taskfile.dist.yml):

````shell
REGISTRY=docker.gbif.org task docker:push:dist
````

This builds `linux/amd64` and `linux/arm64` images, pushes them, and creates a Docker manifest tagged with the
`crawler-cli` Maven `project.version`. The Helm chart's `image.tag` should match.

### Deploying to a Kubernetes cluster

Each key under `commands` in [`helm/values.yaml`](helm/values.yaml) becomes one `Deployment` or `Job` named after that
key. The nested `command` value must match the Java CLI name (see `super("…")` in each `*Command.java` under
`crawler-cli`); names are not all formatted the same (`dwcdpdownloader` vs `dwcdp-metasync`).

Typical commands include crawl coordination (`scheduler`, `coordinator`, `crawlserver`), DwC-A (`downloader`,
`validator`, `dwca-metasync`), DwC-DP / CoL-DP (`dwcdpdownloader`, `dwcdp-metasync`, `coldpdownloader`,
`coldp-metasync`), ABCD-A (`abcdadownloader`), CamtrapDP (`camtrapdpdownloader`, `camtrapdptodwca`), legacy `metasync`,
`coordinatorcleanup`, and Jobs such as `startcrawl` / `crawleverything` / `metasynceverything` / `pusheml`. Use
[`helm/values-dev.yaml.example`](helm/values-dev.yaml.example) as a second values file with placeholder hosts and NFS
paths; copy it to `helm/values-dev.yaml`, replace placeholders, and pass it with `-f` on `helm template` / `helm upgrade`
(Helm merges it over the chart defaults).

The steps below use namespace `dev` and release name `crawler-cli`; adjust to your conventions.

1. Confirm the kubectl context points at the right cluster:

   ````shell
   kubectl config current-context
   kubectl get nodes
   ````

2. Create the namespace if it does not already exist:

   ````shell
   kubectl create namespace dev
   ````

3. Create RabbitMQ and Registry credentials as Secrets:

   ````shell
   kubectl -n dev create secret generic crawler-rabbitmq-credentials \
     --from-literal=username=<user> --from-literal=password=<pass>

   kubectl -n dev create secret generic crawler-registry-credentials \
     --from-literal=username=<user> --from-literal=password=<pass>
   ````

4. Build and push a multi-arch image (or use CI), then set `image.repository`, `image.name`, and `image.tag` in your override to match:

   ````shell
   REGISTRY=docker.gbif.org task docker:push:dist
   ````

5. Copy the committed template and edit hosts, ZooKeeper, registry URL, volumes, and toggles:

   ````shell
   cp helm/values-dev.yaml.example helm/values-dev.yaml
   $EDITOR helm/values-dev.yaml
   ````

6. Render and install:

   ````shell
   helm template crawler-cli ./helm -f helm/values-dev.yaml -n dev | less
   helm upgrade --install crawler-cli ./helm -f helm/values-dev.yaml -n dev
   ````

7. Check workloads (use your enabled command keys as deployment names):

   ````shell
   kubectl -n dev get deploy,job
   kubectl -n dev get pods
   kubectl -n dev logs deploy/<command-key> -f
   kubectl -n dev exec deploy/<command-key> -- cat /app/.tmp/crawler.yaml
   ````

8. Roll back or uninstall:

   ````shell
   helm rollback crawler-cli -n dev
   helm uninstall crawler-cli -n dev
   ````

## Sequence

### Darwin Core Archive
 * Downloader
   * Validator
     * Metasync
       * Pipelines (all archives)
       * Normalizer (Checklist)

More information in crawler-cli [README](crawler-cli/README.md).

### Darwin Core Data Package

Darwin Core Data Package crawling is split across the crawler CLI and the separate
[dwc-dp-analyser-service](https://github.com/gbif/dwc-dp-analyser-service/tree/dev):

 * `dwcdpdownloader` downloads the archive to shared storage and publishes a `DwcDpDownloadFinishedMessage`.
 * `dwc-dp-analyser-service` is deployed from its own repository/chart, validates the archive, and publishes a
   `DwcDpValidationFinishedMessage`.
 * `dwcdp-metasync` listens for validation-finished messages, forwards metadata to the Registry, and publishes a
   `DwcDpMetadataSyncFinishedMessage` after metadata sync succeeds.

The downloader, validator, and metasync services must all agree on the shared archive storage backing the DwC-DP
archive paths.

### Catalogue of Life Data Package

 * `coldpdownloader` downloads the archive to shared storage and publishes a `ColDpDownloadFinishedMessage`.
 * `coldp-metasync` listens for the download-finished message and forwards CoL-DP metadata to the Registry.
