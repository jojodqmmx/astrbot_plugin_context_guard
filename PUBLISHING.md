# 发布到 AstrBot 插件市场

AstrBot 官方流程是：先将插件代码推送到 GitHub 仓库，再登录 AstrBot Cloud 的插件发布页面提交插件。提交前请完成以下操作：

1. 在 GitHub 创建一个公开仓库，建议仓库名为 `astrbot_plugin_context_guard`。
2. 将本目录中的项目文件放在仓库根目录；不要上传 `.git/`、`__pycache__/`、虚拟环境或个人数据库。
3. 编辑 `metadata.yaml`：
   - `author` 改成你希望公开显示的作者名；
   - `repo` 改成真实 GitHub 仓库地址，不能保留 `your-name` 示例地址。
4. 检查 README 中的版本、配置说明和兼容性声明。
5. 推送到 GitHub 后，注册/登录 AstrBot Cloud，在插件发布页面提交仓库。
6. 发布后如需更新，提升 `metadata.yaml` 和 `CHANGELOG.md` 中的版本号，再推送新版本并按市场流程更新。

官方发布说明：
https://docs.astrbot.app/dev/star/plugin-publish.html

官方说明指出，插件市场 ZIP 不得超过 16 MB；本插件不依赖第三方 Python 包，发布包体积远低于该限制。

注意：当前 `metadata.yaml` 的仓库地址是占位示例。替换为自己的 GitHub 仓库并完成市场提交前，不要把它当作已发布插件。
