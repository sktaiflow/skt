import requests
import click


nes_url = "http://nes.sktai.io/v1/runs"


def nes_submit(
    input_notebook, parameters=None,
):
    data = {}
    data["input_url"] = input_notebook
    if parameters:
        data["parameters"] = parameters
    res = requests.post(nes_url, json=data)
    print(f"Job submitted with: {data}")
    res.raise_for_status()
    id = res.json()["id"]
    output_url = res.json()["output_url"]
    print(f"id: {id}")
    print(f"output_url: {output_url}")
    return id, output_url


def nes_get_status(id):
    res = requests.get(f"{nes_url}/{id}")
    res.raise_for_status()
    status = res.json()["status"]
    return status


def nes_execute(
    input_notebook, parameters=None,
):
    """
    return: str
    Succeeeded or Failed or Error
    """
    from time import sleep

    id, output_url = nes_submit(input_notebook, parameters=parameters)
    status = nes_get_status(id)
    poll_interval = 30
    while status in ["Running", "Pending"]:
        print(f'Polling job status... current status: "{status}"')
        sleep(poll_interval)
        status = nes_get_status(id)
    return status, output_url


@click.command(context_settings=dict(help_option_names=["-h", "--help"]))
@click.argument("input_notebook")
@click.option(
    "--parameters", "-p", nargs=2, multiple=True, help="Parameters to pass to the parameters cell.",
)
def nes_cli(
    input_notebook, parameters,
):
    parameters_final = {}
    for name, value in parameters or []:
        parameters_final[name] = value
    status, output_url = nes_execute(input_notebook, parameters=parameters_final,)
    print(f"output_url: {output_url}")
    print(f"status: {status}")
