import happybase
c = happybase.Connection('localhost', 9090)
print(c.tables())