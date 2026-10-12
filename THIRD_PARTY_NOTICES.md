# Third-party notices

The repository includes the following third-party source or bootstrap files.
Their copyright and license terms remain applicable to those files, independently
of any project-level license. Source headers are retained.

| Component | Included files | License | Upstream |
|---|---|---|---|
| Muduo, with local modifications | `muduo/base/`, `muduo/net/` | [BSD-style license](licenses/MUDUO-BSD.txt) | [chenshuo/muduo](https://github.com/chenshuo/muduo) |
| JSON for Modern C++ 3.11.3 | `third_party/json.hpp` | [MIT](licenses/NLOHMANN-JSON-MIT.txt) | [nlohmann/json](https://github.com/nlohmann/json/tree/v3.11.3) |
| Gradle wrapper | `android-app/gradlew`, `android-app/gradlew.bat`, `android-app/gradle/wrapper/` | [Apache-2.0](licenses/GRADLE-APACHE-2.0.txt) | [gradle/gradle](https://github.com/gradle/gradle) |

System packages, container images, Android dependencies, optional model weights,
and external Agent runtimes are installed separately and retain their own terms.
The project's evolution and original project reference are documented in
[project evolution](docs/interview-project-evolution.md).
