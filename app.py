import sys
from confluent_kafka import Consumer, KafkaException, KafkaError
from flask import Flask, render_template_string, jsonify, request
import threading
import logging
import time

app = Flask(__name__)

running = True
messages = []  # Lista global para armazenar as mensagens
partition_filter = "All"
message_order = "Latest"
message_limit = 50

# Configuração de logging
logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)


def msg_process(msg):
    try:
        # Tenta decodificar como UTF-8, se falhar, trata como dados binários
        try:
            value = msg.value().decode('utf-8')
        except UnicodeDecodeError:
            value = repr(msg.value())  # Trate como dados binários

        logger.debug(f'Mensagem recebida: {value}')
        # Adiciona a mensagem à lista de mensagens
        messages.append({
            'Timestamp': msg.timestamp()[1],  # Obtém o timestamp da mensagem
            'Offset': msg.offset(),
            'Partition': msg.partition(),
            'Key': msg.key().decode('utf-8') if msg.key() else None,
            'Value': value
        })
    except Exception as e:
        logger.error(f"Erro ao processar mensagem: {str(e)}")


def basic_consume_loop(consumer, topics):
    try:
        consumer.subscribe(topics)
        logger.debug(f'Subscrito no tópico: {topics}')

        while running:
            msg = consumer.poll(timeout=1.0)
            if msg is None:
                logger.debug("Nenhuma mensagem recebida. Timeout expirado.")
                continue

            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    sys.stderr.write('%% %s [%d] reached end at offset %d\n' %
                                     (msg.topic(), msg.partition(), msg.offset()))
                else:
                    raise KafkaException(msg.error())
            else:
                msg_process(msg)
    finally:
        consumer.close()
        logger.debug("Consumidor Kafka fechado.")


@app.route('/start')
def start_consumer():
    global running
    global messages
    global consumer

    running = True
    messages = []  # Limpa a lista de mensagens antes de começar

    # Cria o Kafka Consumer aqui
    consumer = Consumer({
        'bootstrap.servers': 'pkc-12576z.us-west2.gcp.confluent.cloud:9092',
        'group.id': f'flask-group-{int(time.time())}',  # Gera um group.id único
        'auto.offset.reset': 'earliest',
        'security.protocol': 'SASL_SSL',
        'sasl.mechanism': 'PLAIN',
        'sasl.username': 'PJYTNJYH3BHRULBR',
        'sasl.password': 'xo69CWjBud1o09TeP+cqHxm7c/pV/fYKvosxP7I2igQVPVfHCQ9goxu1rDqKEOlL'
    })

    consumer_thread = threading.Thread(target=basic_consume_loop, args=(consumer, ['sample_data']))
    consumer_thread.start()
    return "Consumer started"


@app.route('/stop')
def stop_consumer():
    global running
    running = False
    return "Consumer stopped"


@app.route('/messages', methods=['GET', 'POST'])
def list_messages():
    global partition_filter, message_order, message_limit

    if request.method == 'POST':
        # Captura os valores dos filtros
        partition_filter = request.form.get('partition', 'All')
        message_order = request.form.get('order', 'Latest')
        message_limit = int(request.form.get('limit', 50))

    filtered_messages = [msg for msg in messages if
                         partition_filter == "All" or str(msg['Partition']) == partition_filter]

    # Ordenação das mensagens
    if message_order == "Latest":
        filtered_messages = sorted(filtered_messages, key=lambda x: x['Timestamp'], reverse=True)
    else:
        filtered_messages = sorted(filtered_messages, key=lambda x: x['Timestamp'])

    # Limitação das mensagens
    limited_messages = filtered_messages[:message_limit]

    # Gera a tabela HTML das mensagens
    table_html = '''
    <div class="container">
        <div class="row">
            <div class="col">
                <strong>Production in last hour:</strong> <span>6.890 messages</span>
            </div>
            <div class="col">
                <strong>Consumption in last hour:</strong> <span>415.819 messages</span>
            </div>
            <div class="col">
                <strong>Total messages:</strong> <span>{len(messages)}</span>
            </div>
            <div class="col">
                <strong>Retention time:</strong> <span>1 week</span>
            </div>
        </div>
    </div>

    <form method="POST" action="/messages">
        <div class="form-row">
            <div class="col">
                <input type="text" class="form-control" placeholder="Filter by timestamp, offset, key or value" name="filter">
            </div>
            <div class="col">
                <select class="form-control" name="partition">
                    <option value="All">All partitions</option>
                    <option value="0">Partition 0</option>
                    <option value="1">Partition 1</option>
                    <option value="2">Partition 2</option>
                    <!-- Adicione outras partições conforme necessário -->
                </select>
            </div>
            <div class="col">
                <select class="form-control" name="order">
                    <option value="Latest">Latest</option>
                    <option value="Earliest">Earliest</option>
                </select>
            </div>
            <div class="col">
                <select class="form-control" name="limit">
                    <option value="50">Max 50 results</option>
                    <option value="100">Max 100 results</option>
                    <option value="200">Max 200 results</option>
                </select>
            </div>
            <div class="col">
                <button type="submit" class="btn btn-primary">Apply Filters</button>
            </div>
        </div>
    </form>

    <table class="table table-striped mt-3">
        <thead>
            <tr>
                <th>Timestamp</th>
                <th>Offset</th>
                <th>Partition</th>
                <th>Key</th>
                <th>Value</th>
            </tr>
        </thead>
        <tbody>
    '''
    for msg in limited_messages:
        table_html += f'''
            <tr>
                <td>{msg['Timestamp']}</td>
                <td>{msg['Offset']}</td>
                <td>{msg['Partition']}</td>
                <td>{msg['Key']}</td>
                <td>{msg['Value']}</td>
            </tr>
        '''
    table_html += '''
        </tbody>
    </table>
    <div>
        <a href="/messages" class="btn btn-primary mt-3">Atualizar</a>
        <span class="ml-2">Total de mensagens: {len(messages)}</span>
    </div>
    '''
    return render_template_string(f'''
    <!DOCTYPE html>
    <html lang="en">
    <head>
        <meta charset="UTF-8">
        <meta name="viewport" content="width=device-width, initial-scale=1.0">
        <link rel="stylesheet" href="https://stackpath.bootstrapcdn.com/bootstrap/4.3.1/css/bootstrap.min.css">
        <title>Kafka Messages</title>
    </head>
    <body>
        <div class="container">
            <h1 class="mt-5">Messages from Kafka Topic: sample_data</h1>
            {table_html}
        </div>
    </body>
    </html>
    ''')


if __name__ == '__main__':
    app.run(debug=True, host='0.0.0.0', port=5000)
