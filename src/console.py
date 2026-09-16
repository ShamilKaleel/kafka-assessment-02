"""Colored terminal output shared by all scripts.

One color per kind of event, so a viewer can follow the demo at a glance:
  green      sent / received (success)
  yellow     retry attempt
  bold green recovered after a retry
  bold red   routed to the DLQ / decode error
  cyan       information and banners
Colors switch off automatically when output is not a terminal (e.g. logs).
"""

from rich.console import Console
from rich.panel import Panel
from rich.table import Table

# markup/highlight off: our lines contain "[p0 @3]"-style brackets and numbers
# that rich would otherwise interpret as markup or recolor.
console = Console(markup=False, highlight=False, soft_wrap=True, emoji=False)

GREEN, YELLOW, RED, CYAN = "green", "yellow", "bold red", "cyan"


def banner(title: str, **fields):
    body = "\n".join(f"{k:<12} {v}" for k, v in fields.items())
    console.print(Panel(body, title=title, border_style=CYAN, expand=False))


def info(text: str):
    console.print(text, style=CYAN)


def sent(order: dict, topic: str, partition: int, offset: int, tag: str = ""):
    console.print(
        f"SENT      #{order['orderId']} {order['product']:<6} ${order['price']:>7.2f}"
        f"  -> {topic} [p{partition} @{offset}]{_tag(tag)}",
        style=GREEN,
    )


def received(order: dict, average: float, count: int, partition: int, offset: int):
    console.print(
        f"RECEIVED  #{order['orderId']} {order['product']:<6} ${order['price']:>7.2f}"
        f"  avg ${average:>7.2f} (n={count})  [p{partition} @{offset}]",
        style=GREEN,
    )


def retry(order_id: str, attempt: int, max_attempts: int, reason: str, delay: float):
    console.print(
        f"RETRY     #{order_id} attempt {attempt}/{max_attempts} failed: {reason}"
        f" - retrying in {delay}s",
        style=YELLOW,
    )


def recovered(order_id: str, attempt: int):
    console.print(f"RECOVERED #{order_id} succeeded on attempt {attempt}", style="bold green")


def gave_up(order_id: str, attempts: int, reason: str):
    console.print(f"FAILED    #{order_id} attempt {attempts}/{attempts} failed: {reason} - giving up", style=RED)


def dlq(key: str, topic: str, partition: int, offset: int):
    console.print(f"DLQ       #{key} -> {topic} [p{partition} @{offset}]", style=RED)


def error(text: str):
    console.print(text, style=RED)


def summary(processed: int, recovered_count: int, dlq_count: int, average: float):
    table = Table(title="Consumer summary", title_style=CYAN, show_header=False, border_style=CYAN)
    table.add_row("orders processed", str(processed), style=GREEN)
    table.add_row("  recovered after retry", str(recovered_count), style="bold green")
    table.add_row("sent to DLQ", str(dlq_count), style=RED)
    table.add_row("final running average", f"${average:.2f}")
    console.print(table)


def dlq_table(messages, topic: str):
    table = Table(title=f"Dead Letter Queue: {topic}", title_style=RED, border_style=RED)
    table.add_column("offset", justify="right")
    table.add_column("key")
    table.add_column("reason")
    table.add_column("came from")
    table.add_column("order")
    for m in messages:
        if m.order is not None:
            order = f"#{m.order['orderId']} {m.order['product']} ${m.order['price']:.2f}"
        else:
            order = f"undecodable bytes: {m.raw!r}"
        table.add_row(str(m.offset), m.key, m.reason, m.origin, order)
    console.print(table)
    console.print(f"{len(messages)} message(s) in {topic}", style=CYAN)


def _tag(tag: str) -> str:
    return f"  ({tag})" if tag else ""
