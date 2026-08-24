# CI/CD 문서

현재 저장소에는 GitHub Actions **CI만 구현**되어 있으며 자동 배포 workflow는 없다.

| 문서 | 상태 | 내용 |
| --- | --- | --- |
| [GitHub Actions와 배포 방식](./github-actions-deploy-options.md) | CI 운영 / CD 미결정 | 현재 CI, 수동 배포 경로, CD 선택지와 도입 조건 |

실제 동작의 최종 기준은 다음 파일이다.

- CI workflow: `.github/workflows/ci.yml`
- 검증 명령: `Makefile`의 `sync-ci-*`, `test-ci-*`
- 운영 배포 명령: `Makefile`의 `deploy-*`
- 운영 서비스 구성: `ops/compose/docker-compose.prod.yml`
- AWS 접속·권한: `terraform/`
