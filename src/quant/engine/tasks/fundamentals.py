from quant.engine.stages import fundamentals as fundamentals_stage

def run(engine):
    fundamentals_stage.run(engine)

run.dependencies = ["prices"]