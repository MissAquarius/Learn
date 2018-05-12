# -*- coding:utf-8 -*-

from pymongo import MongoClient
from bson import json_util as jbson

client = MongoClient('localhost', 27017)

# 连接StudentInfo数据库，没有的话，自动创建
db = client.StudentInfo
# 使用students集合，没有的话，自动创建
students = db.students


# insert
students.insert_one({"name": "Jane",
                     "gender": "female",
                     "age": "18",
                     "department": "computer",
                     "location": {"country": "American", "city": "NewYork"}  # 嵌套文档
                     })

students.insert_many([{"name": "Mary",
                       "gender": "female",
                       "age": "19",
                       "department": ["english", "biology"],  # 一个key对应多value
                      "location": {"country": "American", "city": "Washington"}},
                     {"name": "LiHua",
                      "gender": "male",
                      "age": "17",
                      "department": "information",
                      "location": {"country": "China", "city": "WuHan"}},
                      {"name": "ZhangWei",
                      "gender": "male",
                       "age": "19",
                       "department": "information",
                       "location": None},
                      {"name": "ZhangWei",
                      "gender": "male",
                       "age": "19",
                       "department": "information"}])

# insert_many()等同于
StudentList = [{"name": "Jack",
                "gender": "male",
                "age": "19",
                "department": ["chemistry", "information"],
                "location": {"country": "Canada", "city": "Ottawa"}},
               {"name": "HanMeiMei",
                "gender": "female",
                "age": "17",
                "department": "biology",
                "location": {"country": "China", "city": "Beijing"}}]

try:
    for student in StudentList:
        students.insert(student)
except Exception as e:
    print(e)

# 查询
# 查询集合中的所有文档
# select * from students
for student in students.find():
    print(student)

# 查询集合中的特定文档
# select * from students where name="Jack"
print(jbson.dumps(students.find({"name": "Jack"})))

for student in students.find({"name": "Jack"}):
    print(student)

# 查询内嵌内档
# 可以用json的方法
for student in students.find({"location.country": "Canada"}):
    print(student)


# 查询数组
for student in students.find({"department": {"$all": ["chemistry", "information"]}}):
    print(student)

# in/not in 查询
# select * from students where department [not]in ("english", "computer")
for student in students.find({"department": {"$in": ["english", "computer"]}}):
    print(student)

for student in students.find({"department": {"$nin": ["english", "computer"]}}):
    print(student)  

# and 查询
# select * from students where gender=male and age<=19
for student in students.find({"gender": "male", "age": {"$lte": "19"}}):
    print(student)

# or 查询
# select * from students where gender=male or age>19
for student in students.find({"$or": [{"gender": "male"}, {"age": {"$gt": "19"}}]}):
    print(student)

# and 和 or 组合查询
# select * from students where gender=female and (age=19 or department like 'c%')
for student in students.find({"gender": "female", "$or": [{"age": "19"}, {"department": {"$regex": "^c"}}]}):
    print(student)

# 查询返回文档的某几个属性,默认_id显示
# select id, name, department from students where age>=19
for student in students.find({"age": {"$gte": "19"}}, {"name": 1, "department": 1}):
    print(student)


# select name, department from students where age>=19
for student in students.find({"age": {"$gte": "19"}}, {"name": 1, "department": 1, "_id": 0}):
    print(student)

# select name, department, location.country from students where age>=19
for student in students.find({"age": {"$gte": "19"}}, {"name": 1, "department": 1, "location.country": 1, "_id": 0}):
    print(student)

# 空值查询，不加exists将会把属性不存在的文档也查出来
# select name,age from students where location is NULL
for student in students.find({"location": None}, {"name": 1, "age": 1, "location": 1, "_id": 0}):
    print(student)

for student in students.find({"location": {"$in": [None], "$exists": True}}, {"name": 1, "age": 1, "location": 1, "_id": 0}):
    print(student)


# 查询结果排序
# select name,age from students order by age ASC
for student in students.find({}, {"name": 1, "age": 1, "_id": 0}).sort([("age", 1)]):
    print(student)


# select name,age from students order by age DESC
for student in students.find({},{"name": 1, "age": 1, "_id": 0}).sort([("age", -1)]):
    print(student)

# 查询结果计数
# select count(*) from students where age<19
print(students.find({"age": {"$lt": "19"}}).count())

# 返回查询结果前3行
# select top 3 from students where age>=18
for student in students.find({"age": {"$gte": "18"}}).limit(3):
    print(student)


# 修改
# 单个修改
# update students set location.country="China" and location.city="TaiWang" where name="LiHua"
print(students.find_one({"name": "LiHua"}))
students.update_one({"name": "LiHua"}, {"$set": {"location": {"country": "China", "city": "TaiWang"}}})
print(students.find_one({"name": "LiHua"}))


# 多个修改
# update students set department="informationScience" where department in ("information")
for student in students.find({"department": {"$in": ["information"]}}):
    print(student)
students.update_many({"department": {"$in": ["information"]}}, {"$set": {"department": "informationScience"}})
for student in students.find({"department": {"$regex": "^information"}}):
    print(student)

# repalce_one替换整个文档，除了_id
print(students.find_one({"name": "LiHua"}))
students.replace_one({"name": "LiHua"}, {"name": "LiHua", "gender": "male", "age": 17, "department": "information"})
print(students.find_one({"name": "LiHua"}))


# 删除
# 单个删除
# delete from students where name="LiHua"
print(students.find_one({"name": "LiHua"}))
students.delete_one({"name": "LiHua"})
print(students.find_one({"name": "LiHua"}))

# 删除多个
# delete from students where age<=18
print(students.find({"age": {"$lte": "18"}}).count())
students.delete_many({"age": {"$lte": "18"}})
print(students.find({"age": {"$lte": "18"}}).count())

# delete from students
print(students.count())
students.delete_many({})
print(students.count())

# drop students
if(db.drop_collection('students')):
    print("Success to delete the collection of students!")


# drop StudentInfo
if(client.drop_database("StudentInfo")):
    print("Success to delete the database of StudentInfo!")
