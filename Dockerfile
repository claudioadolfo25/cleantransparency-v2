steps:
# ---------- Build ----------
- name: gcr.io/cloud-builders/docker
  args: [
    "build",
    "-f", "Dockerfile",
    "-t", "us-central1-docker.pkg.dev/$PROJECT_ID/cleantransparency-v2/cleantransparency-v2-image:latest",
    "."
  ]
# ---------- Push ----------
- name: gcr.io/cloud-builders/docker
  args: [
    "push",
    "us-central1-docker.pkg.dev/$PROJECT_ID/cleantransparency-v2/cleantransparency-v2-image:latest"
  ]
# ---------- Deploy ----------
- name: gcr.io/google.com/cloudsdktool/cloud-sdk
  entrypoint: gcloud
  args:
    - run
    - deploy
    - cleantransparency-v2
    - --image=us-central1-docker.pkg.dev/$PROJECT_ID/cleantransparency-v2/cleantransparency-v2-image:latest
    - --region=us-central1
    - --platform=managed
    - --allow-unauthenticated
    # MOUNT SECRET FILE (formato corregido)
    - --update-secrets=/secrets/p12_certificado_v2=p12_certificado_v2:latest
    # SECRET ENV VAR
    - --update-secrets=P12_PASSWORD=p12_pwd_v2:latest
    # Extra envs
    - --set-env-vars=ENVIRONMENT=production
timeout: "1200s"
options:
  logging: CLOUD_LOGGING_ONLY
