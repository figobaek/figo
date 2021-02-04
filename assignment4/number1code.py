# -*- coding: utf-8 -*-

# 1. 헤더가 레코드로 추가되는 문제 해결하기

def transform(text):
    lines = text.split("\n")
    lines = lines[1:] # 헤더빼고 변수 지정
    return lines

# 2, 3. Idempotent하게 잡을 만들기, transaction 사용하기

# 1) 테이블 밖에서 생성, 함수에서 값만 넣어주기 

def load(lines):
    cur = get_Redshift_connection()
    sql = "BEGIN; DELETE FROM goldenboyy0524.name_gender;"
    try:
      for r in lines:
          if r != '':
              (name, gender) = r.split(",")
              print(name, "-", gender)
              sql += "INSERT INTO goldenboyy0524.name_gender VALUES ('{name}', '{gender}');".format(name=name, gender=gender)
      sql += "END;"
    except:
      sql += "ROLLBACK;"
    print(sql)
    cur.execute(sql)

# 2) 함수 안에서 이전 테이블 전체 삭제 -> 생성 -> 값 넣어주기 

def load(lines):

    cur = get_Redshift_connection()
    sql = "BEGIN; DROP TABLE goldenboyy0524.name_gender; CREATE TABLE goldenboyy0524.name_gender ( name varchar(32), gender varchar(8));"
    try:
      for r in lines:
          if r != '':
              (name, gender) = r.split(",")
              print(name, "-", gender)
              sql += "INSERT INTO goldenboyy0524.name_gender VALUES ('{name}', '{gender}');".format(name=name, gender=gender)
      sql += "COMMIT;"
    except:
      sql += "ROLLBACK;"
    print(sql)
    cur.execute(sql)
