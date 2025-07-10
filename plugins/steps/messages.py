from airflow.providers.telegram.hooks.telegram import TelegramHook

def send_success_message(context):
    """
    Send a success message to Telegram when the DAG execution succeeds.
    """
    hook = TelegramHook(token='6299281862:AAF6CR7N4t1OWWz7QovRTaJHzChvrlX2m4o', chat_id='278945803')
    dag = context['dag'].dag_id
    run_id = context['run_id']
    
    message = f'Исполнение DAG {dag} с id={run_id} прошло успешно!'
    hook.send_message({
        'chat_id': '{вставьте ваш chat_id}',
        'text': message
    })

def send_failure_message(context):
    """
    Send a failure message to Telegram when the DAG execution fails.
    """
    hook = TelegramHook(token='6299281862:AAF6CR7N4t1OWWz7QovRTaJHzChvrlX2m4o', chat_id='278945803')
    dag = context['dag'].dag_id
    run_id = context['run_id']
    task_instance_key_str = context['task_instance_key_str']
    
    message = f'Исполнение DAG {dag} ({task_instance_key_str}) с id={run_id} провалилось!'
    hook.send_message({
        'chat_id': '{вставьте ваш chat_id}',
        'text': message
    })