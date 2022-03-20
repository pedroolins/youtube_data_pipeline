import pymsteams

webhook = 'wats'
webhook_error = "s"

def _send(message, webhook, error=False):
    m = pymsteams.connectorcard(webhook)
    m.text(message)
    if error:
        m.color("#580505")
    m.send()


def send_message(message, error=False):
    _send(message, webhook, error)
    if error:
        _send(message, webhook_error, True)

if __name__ == "__main__":
    print(send_message('daleee jogadorrr, testando essa budega'))
