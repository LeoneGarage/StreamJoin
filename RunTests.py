test_notebooks = [
"./tests/JoinTestInner",
"./tests/JoinTestRight",
"./tests/JoinTestLeft",
"./tests/JoinTestInnerRight",
"./tests/JoinTestRightInner",
"./tests/JoinTestInnerLeft",
"./tests/JoinTestLeftInner",
"./tests/JoinTestLeftLeft",
"./tests/JoinTestRightRight",
"./tests/JoinTestRightLeft",
"./tests/JoinTestLeftRight",
"./tests/JoinTestInnerInnerInner",
"./tests/JoinTestLeftRightInner",
"./tests/JoinTestInnerInnerLeft",
"./tests/JoinTestRightRightLeft",
"./tests/JoinTestLeftInnerRight",
"./tests/JoinTestLeftRightLeft",
"./tests/JoinTestComplex1",
"./tests/AggsTestGroupBy",
"./tests/AggsTestRightGroupBy",
"./tests/AggsTestInnerGroupByLeft",
"./tests/AggsTestInnerGroupByLeftLeftGroupBy",
"./tests/AggsTestRightGroupByInnerGroupBy",
"./tests/AggsTestRightGroupByInnerGroupByMax"
]

index = 1
for test in test_notebooks:
  print(f'Running Test "{test}", {index} of {len(test_notebooks)}')
  dbutils.notebook.run(test, 0)
  index += 1

print(f"{len(test_notebooks)} Tests completed succesfully")
