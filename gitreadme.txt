# 清理git锁
rm .git/index.lock -Force
# 查看状态
git status
# 重新提交你的修改
git add .
git commit -m "fix: 清理Git锁，恢复提交"
# 推送
git push
