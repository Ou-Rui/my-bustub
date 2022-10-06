# Project4 Concurrency Control
## 22.10.05
- PASS grading_lock_manager_test_1.cpp BasicTest

## 22.10.06
- PASS grading_lock_manager_test_2.cpp TwoPLTest

# Project3 Query Execution
## 22.9.13
- PASS catalog_test
- PASS autograder

## 22.10.1
- PASS grading_executor_test.cpp

# Project2 B+Tree
## 22.7.20
- PASS Concurrent Insert1/2

## 22.7.17
- 初步写完了Delete和Iterator，通过DeleteTest1/2
  - ScaleTest还不行，OOR
- ScaleTest PASS

## 22.7.15
- 通过Checkpoint1
- Checkpoint1节点内优化为二分查找

## 22.7.13
- 完成Split_Test

## 22.7.12
- 一头雾水，准备先看checkpoint1 test code

# Project1: Buffer Pool
## 22.7.12
- 批量测试脚本完成，测试500次通过

## 22.7.7
- 移植了2021的代码到2020版本，修了几个bug，通过远端测试
  - NewPage时要先写入磁盘
  - Flush时不应该仅Flush Dirty Page
  - 提交网站时应关闭LOGGING

## 22.7.6
- Task1改为双向链表+哈希表的数据结构
- Task2通过GradeScope测试

## 22.7.4
- Task1完成，感觉不一定没有bug
- Task1 GradeScope通过

# Project0: C++ Primer
## 22.7.7
- 切换到2020的版本，通过GradeScope测试
## 22.7.1
- 通过InitializationTest
    - 基类Matrix
    - 派生类RowMatrix的构造、析构、填入元素、读取元素
- 通过ElementAccessTest
    - 增加了写入元素
- 通过AdditionTest和MultiplicationTest
    - 完成了RowMatrixOperations类