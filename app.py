import os
import zipfile
import time
from datetime import timedelta
from io import BytesIO
from shutil import copyfile

from flask import Flask, render_template, request, redirect, session, jsonify, send_from_directory, Config, send_file, \
    make_response
from core.HqlParse import HqlParse
import json
from tools import HqlToER, Hive_2_Excel
from tools.SQlBuilder import SQLBuilder


import socket

def fqdn_ip():
    return {
        'fqdn_ip': socket.gethostbyname(socket.getfqdn())
    }



app = Flask(__name__)
app.secret_key = os.urandom(24)
app.config['SEND_FILE_MAX_AGE_DEFAULT'] = timedelta(seconds=5)

TMP_PATH = 'D:/tmp/'

BUG_PATH = 'D:/tmp/bug/'

cur_dir = 'D:/target/txt'

def read_file_to_sql(m_file):
    hql = ''
    with open(m_file, encoding='utf-8') as f:
        hql = f.read()
    return hql

def write_file(sql):
    if not os.path.exists(TMP_PATH):
        os.mkdir(TMP_PATH)

    if not os.path.exists(cur_dir):
        os.mkdir(cur_dir)

    filepath = TMP_PATH  + str(time.time()) + '.txt'
    with open(filepath, 'w',encoding='utf-8',newline='') as f:
        f.write(sql)
    return filepath

def write_bug_file(sql):
    if not os.path.exists(BUG_PATH):
        os.mkdir(BUG_PATH)
    filepath = BUG_PATH  + str(time.time()) + '.txt'
    with open(filepath, 'w',encoding='utf-8',newline='') as f:
        f.write(sql)
    return filepath


@app.route('/')
def index():
    ipaddr = fqdn_ip()
    return render_template("index.html", ipaddr = ipaddr['fqdn_ip'])

@app.route('/parseSqlFromStr' , methods=['POST'])
def parseSqlFromStr():
    sql = request.form["sql"]
    if sql:
        #session.clear()
        session['sql'] = write_file(sql)
        return redirect('/main')
    else:
        return redirect('/')



@app.route('/main')
def main():
    # if session.get('hql_parse'):
    #     print(session['hql_parse'])
    return render_template("main.html")


@app.route('/hive_to_er')
def hive_to_er():
    cur_dir = 'D:/target/er'
    SQL = read_file_to_sql(session.get('sql'))
    filename_list = HqlToER.hive2ER_from_str(SQL)
    filename_list = [filename + '.jpg' for filename in filename_list]

    dl_name = '{}.zip'.format(filename_list[0])
    if len(filename_list) == 1:
        return send_from_directory(cur_dir, filename_list[0], as_attachment=True)
    else:
        memory_file = BytesIO()
        with zipfile.ZipFile(memory_file, "w", zipfile.ZIP_DEFLATED) as zf:
            for _file in filename_list:
                with open(os.path.join(cur_dir, _file), 'rb') as fp:
                    zf.writestr(_file, fp.read())
        memory_file.seek(0)
        return send_file(memory_file, attachment_filename=dl_name, as_attachment=True)


@app.route('/build_data_verify')
def data_divergence_judge():
    SQL = read_file_to_sql(session.get('sql'))
    sql_builder = SQLBuilder(SQL)
    judge_str = sql_builder.repeat_judge(SQL)
    filename = str(time.time()) + '.txt'
    with open(cur_dir + '/' +  filename,'w',encoding='utf-8') as f:
        f.write(judge_str)
    return send_from_directory(cur_dir, filename, as_attachment=True)


@app.route('/build_test_report')
def build_test_report():
    SQL = read_file_to_sql(session.get('sql'))
    sql_builder = SQLBuilder(SQL)
    judge_str = sql_builder.build_test_sql(SQL)
    filename = str(time.time()) + '.txt'
    with open(cur_dir + '/' +  filename,'w',encoding='utf-8') as f:
        f.write(judge_str)
    return send_from_directory(cur_dir, filename, as_attachment=True)


@app.route('/hive_to_excel')
def hive_to_excel():
    # try:
    SQL = read_file_to_sql(session.get('sql'))
    filepath,filename = Hive_2_Excel.hive_2_excel(SQL)
    return send_from_directory(filepath, filename, as_attachment=True)
    # except Exception as e:
    #     print(dir(e))
    #     print(e.args)
    #     return "你应该没有上传建表语句，或者上传语句有错误。"

@app.route('/uploadbug',methods=['POST'])
def uploadbug():
    sql = request.form["sql"]
    if sql:
        write_bug_file(sql)
    return "提交成功，谢谢反馈。"


@app.route("/upload_file", methods=['POST', 'GET'])
def filelist1():
    file = request.files['file']
    upload_path = TMP_PATH + file.filename
    file.save(upload_path)
    session['sql'] =  upload_path
    res = {'msg':'上传成功','code':200}
    return jsonify(res)


@app.route('/dumplicatesql' , methods=['POST', 'GET'])
def dumplicatesql():
    hostname = socket.gethostname()
    t = time.localtime()
    now = '{0}-{1}-{2}'.format(t.tm_year, t.tm_mon, t.tm_mday)
    SQL = read_file_to_sql(session.get('sql'))
    sql_builder = SQLBuilder(SQL)
    judge_str = sql_builder.column_dumplicate(SQL,hostname,now)
    filename = str(time.time()) + '.txt'
    with open(cur_dir + '/' + filename, 'w', encoding='utf-8') as f:
        f.write(judge_str)
    return send_from_directory(cur_dir, filename, as_attachment=True)

@app.route('/none_dumplicatesql' , methods=['POST', 'GET'])
def none_dumplicatesql():
    hostname = socket.gethostname()
    t = time.localtime()
    now = '{0}-{1}-{2}'.format(t.tm_year, t.tm_mon, t.tm_mday)
    SQL = read_file_to_sql(session.get('sql'))
    sql_builder = SQLBuilder(SQL)
    judge_str = sql_builder.column_none_dumplicate(SQL,hostname,now)
    filename = str(time.time()) + '.txt'
    with open(cur_dir + '/' + filename, 'w', encoding='utf-8') as f:
        f.write(judge_str)
    return send_from_directory(cur_dir, filename, as_attachment=True)

# 多表UNION
@app.route('/tables_union' , methods=['POST', 'GET'])
def tables_union():
    SQL = read_file_to_sql(session.get('sql'))
    ipaddr = request.remote_addr
    hostname =  socket.gethostname()
    t = time.localtime()
    now = '{0}-{1}-{2}'.format(t.tm_year, t.tm_mon, t.tm_mday)
    SQL = read_file_to_sql(session.get('sql'))
    sql_builder = SQLBuilder(SQL)
    judge_str = sql_builder.table_union(SQL,hostname,now)
    filename = str(time.time()) + '.txt'
    with open(cur_dir + '/' + filename, 'w', encoding='utf-8') as f:
        f.write(judge_str)
    return send_from_directory(cur_dir, filename, as_attachment=True)


@app.route('/hql2mysql', methods=['POST', 'GET'])
def hive2mysql():
    SQL = read_file_to_sql(session.get('sql'))
    sql_builder = SQLBuilder(SQL)
    judge_str = sql_builder.hive2Mysql(SQL)
    filename = str(time.time()) + '.txt'
    with open(cur_dir + '/' + filename, 'w', encoding='utf-8') as f:
        f.write(judge_str)
    return send_from_directory(cur_dir, filename, as_attachment=True)

@app.route('/select_generate_page')
def select_generate_page():
    # if session.get('hql_parse'):
    #     print(session['hql_parse'])
    return render_template("select_generate.html")

@app.route('/select_generate', methods=['POST', 'GET'])
def select_generate():
    SQL = read_file_to_sql(session.get('sql'))
    query = request.form["sql"]
    sql_builder = SQLBuilder(SQL)
    judge_str = sql_builder.select_generate(SQL,query=query)
    filename = str(time.time()) + '.txt'
    with open(cur_dir + '/' + filename, 'w', encoding='utf-8') as f:
        f.write(judge_str)
    return send_from_directory(cur_dir, filename, as_attachment=True)

# 获取json 数据 返回json数据
@app.route('/get_insert_date', methods=['POST'])
def get_insert_data():
    # SQL = request.form["sql"]
    data = json.loads(request.get_data())
    SQL = data['sql']
    hql_parse = HqlParse(SQL)
    insert_info = hql_parse.insert_info
    return jsonify(insert_info)

@app.route('/get_create_date', methods=['POST'])
def get_create_data():
    data = json.loads(request.get_data())
    SQL = data['sql']
    hql_parse = HqlParse(SQL)
    tables = hql_parse.tables
    create_info = []
    for table in tables:
        definitions = []
        for column in table['definitions']:
            dic = {}
            dic['column_name'] = column[0].value
            dic['column_type'] = column[1].value
            dic['column_comment'] = column[3].value
            definitions.append(dic)
        table['definitions'] = definitions
        create_info.append(table)
    return jsonify(create_info)


# from wsgiref.simple_server import make_server
# if __name__ == '__main__':
#     server = make_server(host='0.0.0.0', port=8890, app=app)
#     server.serve_forever()

## 持久的服务器
# from gevent import pywsgi

# if __name__ == '__main__':
#     server = pywsgi.WSGIServer(('0.0.0.0', 5000), app)
#     server.serve_forever()

#开发服务器
if __name__ == "__main__":
    app.run(debug = True,host='0.0.0.0',port=5000)
