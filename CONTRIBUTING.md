# Contributing

感谢你参与 NeoEngram。提交改动前，请先搜索现有 issue 和 pull request，避免重复工作。
较大的功能或公共 API 变更，建议先创建 issue 讨论设计和兼容性。

## 本地开发

1. Fork 仓库并从最新默认分支创建短生命周期分支。
2. 进行范围清晰的改动，并为行为变化增加测试和文档。
3. 在提交 pull request 前运行：

```bash
cargo fmt --all -- --check
cargo clippy --workspace --all-targets --all-features -- -D warnings
cargo test --workspace --all-targets --all-features
RUSTDOCFLAGS="-D warnings" cargo doc --workspace --all-features --no-deps
```

## Pull request

- 说明改动解决的问题、实现方式和验证方法。
- 保持每个 pull request 聚焦，避免混入无关重构。
- 如果公共行为发生变化，请同步更新 README 或相关文档。
- 如果涉及路线、架构、研究结论或阶段状态，请同步更新
  [`docs/implementation-plan.md`](docs/implementation-plan.md)；该文件是能力和计划的唯一事实来源。
- 不要求每个 pull request 修改 CHANGELOG；维护者会在发布前统一整理。

## 贡献许可

除非你明确声明其他情况，否则你有意提交并纳入本项目的任何贡献（定义见
Apache-2.0 许可证），均按本项目的 `MIT OR Apache-2.0` 双许可证授权，不附加
额外条款或条件。
