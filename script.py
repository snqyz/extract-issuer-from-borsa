from pathlib import Path

to_delete = [
    "CH1300958894",
    "CH1300965048",
    "DE000SQ43UZ8",
    "DE000SY2Y9J8",
    "DE000UG1N3C3",
    "DE000UG45452",
    "DE000UG4RUY1",
    "DE000UG6GTQ7",
    "DE000VD8UTN3",
    "DE000VD8WSV4",
    "DE000VK1V0S5",
    "GB00BTLDMT38",
    "NLBNPIT279K4",
    "NLBNPIT2A7A5",
    "NLBNPIT2I3V2",
]

folder = Path("isins")

for file in to_delete:
    print(file)
    (folder / f"{file}.txt").unlink()
