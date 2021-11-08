### miebug
灭霸计划
### modules
├── config.json     # 从原来项目拷贝来的配置文件
├── main.py     # 项目的启动文件,  python3 main.py --port=8600 --debug=True
├── model
│   ├── all_model.py    # 项目的所有数据库模型,目前保持只有一个文件不拆分,建立需考虑索引
│   ├── __init__.py
├── README.md    # 项目的说明文档
├── requirements.txt    # 项目的第三方依赖，pip3 install -r requirements.txt
├── routes      # 项目的子路由
│   ├── custom_activity
│       ├── view.py     # view文件，接口注释出完整的api接口
│       └── __init__.py
│   ├── data_fix   # 用于技术支持,研发定位等工具类接口
├── scripts      # 项目的定时脚本
│   ├── __init__.py     # register_scheduler注册脚本函数的地方
│   ├── judge_custom_activity.py
├── service     # 项目的数据库访问方法,后面可以用子目录管理
│   ├── __init__.py     # MBService类，内有创建流水号、参数检查相关、时间戳和datetime转化、nx_lock放并发锁等函数
│   └── user.py
│   └── custom_activity.py
└── utils/mbutils       # 项目的公共方法
    ├── constant        # 项目的常量文件
    │   ├── account.py
    │   ├── activity.py
    │   ├── handler.py
    │   ├── __init__.py     # 所有的返回错误类型ErrorType, 所有的参数验证类型ValidType
    │   ├── response.py
    │   └── user.py
    │   └── redis_key.py    # redis key的文件
    ├── db_manager.py       # 数据库连接库
    ├── __init__.py     # cfg, mb_db_async, MbException, DefaultMaker
    ├── log.py      # 项目的log模块, logger.exception
    ├── mb_handler.py       # 项目的handler库, 所有api接口都是handler来负责处理的,注意验证函数valid_data_all,返回结果success，error
    ├── middle_ware.py      # 项目的中间件
    ├── redis_manager.py    # redis连接库
    ├── route_tool.py   # 项目的路由库
    └── url_mapping.py   # 项目的根路由


# 引入模块顺序
先内置模块，后第三方模块， 再私有(个人)模块，crtl+shift+o 导包格式化

# 代码规范总则
一是代码的可读性，二是代码冗余没有抽象, 三是性能, 四是执行提交前执行mblint.py进行代码检查

# 规范
1.因为是动态语言，成员能够随时添加, 禁止这种行为，要求要在__init__, dict, 这种地方放全部成员，哪怕初始化为None
2.写完代码要格式化代码，   crtl+shift+l
3.因为异步的代码，前期写完接口性能测试
ab -n 1000 -c 100 -H 'Authorization:Bearer eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJvcEFyZWFJZHMiOlsxLDIsMzI2LDMyNywzMzIsNTk4LDEwNzUsMTA3NiwyOTk4LDMxNzksNTY1NCw2ODM4LDY5MDAsNjk4OSw3MTA0XSwidXNlcklkIjoiMTU4NzE2ODgxNjIiLCJwaG9uZSI6IiIsImFnZW50SWQiOjIsImJ1aWxkIjoiIiwiZGV2SWQiOiIiLCJwYWNrYWdlIjoiIiwiaWF0IjoxNTkxNzczNjUyfQ.4LZnkcbWbqfvSILpR9nPHE0hofkvYECzBuwxMzumrO0' http://0.0.0.0:6000/anfu/v2/custom_activity/app/list
4.关键对象通过参数返回值来使对象透明 
_get_arguments(
    self, name: str, source: Dict[str, List[bytes]], strip: bool = True
) -> List[str]
5.接口url，传入参数，传出参数，数据库新表，统一用python风格。
也就是类名称MiebaClass，变量、文件名、目录名、函数名mieba_mie_bug, 枚举常量继承enum.Enum类，名称全部大写MIEBA_COMSTANT
6.时间起止begin，end，翻页page，size
7.返回结果推荐中间位置直接，异常抛出，raise MbException(promt="关键参数id不存在", error_type=ErrorType.BIZ_ERROR)，会被全局捕获返回：
{
    "suc": false,
    "error": {
        "err": "/ebike/device/python?server_id=12&authed=1&phone=123456789011",
        "errType": "BIZ_ERROR",
        "promt": "关键参数id不存在",
        "ErrMessage": "BIZ_ERROR"
    }
}
8.未完成的功能，有坑的地方todo注释 
9.通用的常量放在utils.constant文件，临时的常量可以放在自己模块
10.不用存基础的封装：user = dao_session.session().query(XcEbikeUsrs2).filter_by(id=user_id).first()
11.专门的异常打印方法logger.exception
        try:
            first = dao_session.session().query(XcMieba2CustomActivity).filter_by(id=activity_id).one()
        except Exception:
            raise MbException(promt="活动不存在")
        return first.state, first.reward_type
12.redis 变量规范从DISPATCH_GATHER_TASK = "xc_mieba_gather_{}_{}",
改成DISPATCH_GATHER_TASK = "xc_mieba_gather_{imei}_{report_time}", 提高可读性,
调用DISPATCH_GATHER_TASK.format(imei=imei, report_time=report_time)
13.routes里面的service文件, 不要__init__动作
14.单个文件大小一般控制到1500, 超过时需要考虑拆分, 单文件变成目录
        
# 同步的IO阻塞换成协程方法
协程的写法:
class AppListHandler(MBHandler):
    @coroutine  # 协程函数
    def get(self):
        valid_data = self.valid_data_all([      # 验证参数，类型，必填等数据要求
            ("service_id", ValidType.INT, {"must": True})
        ])
        data = yield mb_async(UserCustomActivityService().query_all)(valid_data)  # 线程装饰器包裹的实际任务
        self.success(data)
任何view里面的耗时操作(特别是网络操作)都需要放在mb_async里面


### python知识补充
1.r'/' ，出现在正则的语句中
2.type, 是内置查看类型函数，取名避免用到，改成tp
3.id，是内置查看地址的字段，取名避免用到， 改成_id或者user_id, activity_id等
4.一个元素的元组的解包
activity_id, = self.valid_data_all([
    ("activity_id", ValidType.INT, {"must": True})
])
5. [].append(1), {}.update({}) 都不是链式的
6.熟悉枚举类用法，3==Enum.State.value  Enum(3)==Enum.State

### 注释,一定要有api完整地址,api的作用,返回结果示例
class BasicDataSummaryHandler(MBHandler):
    """
    api:/anfu/v2/dispatch/app/workman/task_num
    工人进行中的人物数
    :return:
    {
      "suc": true,
      "data": {
        "task_num": 6,
      }
    }
    """

    @coroutine
    def get(self):
        authorization = self.request.xc_basic_info
        op_area_ids = authorization.get("opAreaIds", [])
        agent_ids = authorization.get("agentIds", [])
7.ession.query(User).filter(User.id.in_((1, 2, 3))).delete(synchronize_session=False)
提到了 delete 的一个注意点：删除记录时，默认会尝试删除 session 中符合条件的对象，而 in 操作还不支持，于是就出错了。
解决办法就是删除时不进行同步，然后再让 session 里的所有实体都过期.