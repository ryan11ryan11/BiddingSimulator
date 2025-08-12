from core.models.simulator import gen_15_values, simulate_E_quantiles

def test_simulator_shape():
    vals = gen_15_values(1_000_000_000, 0.02, 100)
    assert len(vals) == 15
    q = simulate_E_quantiles(vals)
    assert set(q.keys()) == {0.5, 0.8, 0.9, 0.95}