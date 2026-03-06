import reflex as rx

config = rx.Config(
    app_name="carnival_reflex",
    frontend_port=3001,
    plugins=[
        rx.plugins.TailwindV4Plugin(),
    ],
)
