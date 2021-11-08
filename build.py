import os
import sys

res = os.system("python3 mblint.py")
if res:
    exit(4)
if len(sys.argv) < 2:
    print("Usage:python3 build.py project [test]")
    exit(2)

project = sys.argv[1]
# os.system("pip3 install -r requirements.txt")
os.system("cd /admin/xcconfig/ && git stash && git reset --hard && git pull")
os.system("cp -rf /admin/xcconfig/ebike/@project/{}/* .".format(project))
# 根据打包参数修改supervisor的env
if len(sys.argv) > 2 and sys.argv[2] == 'test':
    os.system("cd /admin/xcconfig/ && git stash && git reset --hard && git pull")
    os.system("cp -rf /admin/xcconfig/test/ebike/@project/{}/* .".format(project))
    os.system("sed -i 's/python3 main.py/python3 main.py --env=test/' supervisor.conf")
else:
    os.system("cd /admin/xcconfig/ && git stash && git reset --hard && git pull")
    os.system("cp -rf /admin/xcconfig/ebike/@project/{}/* .".format(project))
