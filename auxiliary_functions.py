import bw2data as bd
import bw2calc as bc
from collections import defaultdict


def collect_biosphere_flows(activity, scaling_factor=1, specific_inputs=None, current_tier=0, tier_limit=2):
    """
    Collects biosphere flows and biosphere exchanges up to a specified tier depth.
    Parameters:
    - activity: Brightway activity whose flows are analyzed.
    - scaling_factor: Float, used to scale amounts of flows.
    - specific_inputs: List of names of technosphere inputs to include.
    - current_tier: Int, depth of recursion.
    - tier_limit: Int, maximum depth for recursive search.
    Returns:
    - biosphere_flows: List of tuples, each containing (biosphere flow, scaled amount).
    - biosphere_exchanges: List of biosphere exchanges collected.
    """
    # Check if the current tier exceeds the limit
    if current_tier > tier_limit:
        return [], []
    biosphere_flows = []
    biosphere_exchanges = []
    for exc in activity.exchanges():
        if exc['type'] == 'biosphere':
            # Collect biosphere flow with scaling
            biosphere_flows.append((exc.input, exc.amount * scaling_factor))
            biosphere_exchanges.append(exc)
        elif specific_inputs and exc.input['name'] in specific_inputs and exc['type'] == 'technosphere':
            # If input is in specific_inputs and within tier limit, collect its contributions
            new_scaling_factor = exc.amount * scaling_factor
            nested_flows, nested_exchanges = collect_biosphere_flows(
                exc.input,
                new_scaling_factor,
                specific_inputs,
                current_tier + 1,
                tier_limit
            )
            biosphere_flows.extend(nested_flows)
            biosphere_exchanges.extend(nested_exchanges)
    return biosphere_flows, biosphere_exchanges


def aggregate_flows(biosphere_flows):
    # Use defaultdict to sum amounts for each unique biosphere flow
    aggregated_flows = defaultdict(float)
    for flow, amount in biosphere_flows:
        aggregated_flows[flow] += amount
    # Convert back to list of tuples for final result
    return list(aggregated_flows.items())


def aggregate_technosphere_inputs(activity, scaling_factor=1, specific_inputs=None, current_tier=0, tier_limit=2):
    """
    Aggregates technosphere flows up to a specified tier depth, ensuring all inputs
    from the deepest tier are included, and filtering intermediate tiers by `specific_inputs`.
    Some inputs (e.g., `always_include`) are added directly to the results if not in tier 0.
    Parameters:
    - activity: Brightway activity to analyze.
    - scaling_factor: Float, scales the amount of flows.
    - specific_inputs: List of names of technosphere inputs to explore specifically.
    - current_tier: Int, current depth of recursion.
    - tier_limit: Int, maximum depth for recursive search.
    Returns:
    - aggregated_flows: Dictionary with aggregated inputs and their scaled amounts.
    """
    if current_tier > tier_limit:
        return {}
    aggregated_flows = {}
    # List of inputs that must be directly included in the results
    always_include = [
        "hydrogen production, gaseous, 30 bar, from PEM electrolysis, from grid electricity",
        "market group for electricity, low voltage",
        "hydrogen production, gaseous, 25 bar, from gasification of woody biomass in entrained flow gasifier, "
        "at gasification plant"
    ]
    for exc in activity.exchanges():
        if exc['type'] == 'technosphere':
            # Scale the current input amount
            scaled_amount = exc.amount * scaling_factor
            if exc.input['name'] in always_include and current_tier != 0:
                # Add `always_include` inputs directly to results if not in tier 0
                if exc.input in aggregated_flows:
                    aggregated_flows[exc.input] += scaled_amount
                else:
                    aggregated_flows[exc.input] = scaled_amount
                continue  # Do not explore these inputs further
            if current_tier == tier_limit:
                # Add inputs directly at the last tier
                if exc.input in aggregated_flows:
                    aggregated_flows[exc.input] += scaled_amount
                else:
                    aggregated_flows[exc.input] = scaled_amount
            else:
                # Check if input should be included based on specific_inputs
                if specific_inputs and exc.input['name'] not in specific_inputs:
                    print(f"Skipping {exc.input['name']} at tier {current_tier}")
                    continue
                print(f"Exploring {exc.input['name']} at tier {current_tier}")
                # Recursively explore deeper tiers
                nested_flows = aggregate_technosphere_inputs(
                    exc.input,
                    scaling_factor=scaled_amount,
                    specific_inputs=specific_inputs,
                    current_tier=current_tier + 1,
                    tier_limit=tier_limit
                )
                # Merge nested flows into aggregated flows
                for key, value in nested_flows.items():
                    if key in aggregated_flows:
                        aggregated_flows[key] += value
                    else:
                        aggregated_flows[key] = value
    return aggregated_flows


def rebuild_fuel_acts(act, specific_inputs=None, tier_limit=3):
    """
    It reconstructs the diesel and kerosene acts, so it includes what previously was in all tiers of
    fuel production into tier 0.
    """
    # TODO: check if it works. TEST: lca.score of the old and new activities.
    # TODO: include hydrogen and methanol!
    # TODO: if it works, eliminate 'if' statements from om_biosphere(act).
    # deal with the technosphere
    updated_act = act.copy(database='additional_acts')
    for ex in updated_act.technosphere():
        if ex.input['name'] == specific_inputs[0]:
            ex.delete()
    aggregated_flows = aggregate_technosphere_inputs(
        activity=act, specific_inputs=specific_inputs, tier_limit=tier_limit)
    for act, amount in aggregated_flows.items():
        new_ex = updated_act.new_exchange(input=act, amount=amount, type='technosphere')
        new_ex.save()
    # deal with the biosphere
    if act['name'] == 'diesel production, synthetic, from Fischer Tropsch process, hydrogen from wood gasification, energy allocation, at fuelling station':
        bioflows, bioexchanges = collect_biosphere_flows(
            activity=act,
            tier_limit=3,
            specific_inputs=[
                'diesel production, synthetic, Fischer Tropsch process, hydrogen from wood gasification, energy allocation',
                'syngas, RWGS, Production, for Fischer Tropsch process, hydrogen from wood gasification',
                'carbon monoxide, from RWGS, for Fischer Tropsch process, hydrogen from wood gasification'
            ]
        )
        bioflows_gruped = aggregate_flows(bioflows)
        updated_act.biosphere().delete()
        for flow in bioflows_gruped:
            new_ex = updated_act.new_exchange(input=flow[0], type='biosphere', amount=flow[1])
            new_ex.save()
    elif act['name'] == 'diesel production, synthetic, from Fischer Tropsch process, hydrogen from electrolysis, energy allocation, at fuelling station':
        bioflows, bioexchanges = collect_biosphere_flows(
            activity=act,
            tier_limit=1,
            specific_inputs=[
                'diesel production, synthetic, Fischer Tropsch process, hydrogen from electrolysis, energy allocation',
                ]
        )
        bioflows_gruped = aggregate_flows(bioflows)
        updated_act.biosphere().delete()
        for flow in bioflows_gruped:
            new_ex = updated_act.new_exchange(input=flow[0], type='biosphere', amount=flow[1])
            new_ex.save()
    elif act['name'] == 'kerosene production, synthetic, from Fischer Tropsch process, hydrogen from wood gasification, energy allocation, at fuelling station':
        bioflows, bioexchanges = collect_biosphere_flows(
            activity=act,
            tier_limit=3,
            specific_inputs=[
                'kerosene production, synthetic, Fischer Tropsch process, hydrogen from wood gasification, energy allocation',
                'syngas, RWGS, Production, for Fischer Tropsch process, hydrogen from wood gasification',
                'carbon monoxide, from RWGS, for Fischer Tropsch process, hydrogen from wood gasification'
            ]
        )
        bioflows_gruped = aggregate_flows(bioflows)
        updated_act.biosphere().delete()
        for flow in bioflows_gruped:
            new_ex = updated_act.new_exchange(input=flow[0], type='biosphere', amount=flow[1])
            new_ex.save()
    elif act['name'] == 'kerosene production, synthetic, from Fischer Tropsch process, hydrogen from electrolysis, energy allocation, at fuelling station':
        bioflows, bioexchanges = collect_biosphere_flows(
            activity=act,
            tier_limit=1,
            specific_inputs=[
                'kerosene production, synthetic, Fischer Tropsch process, hydrogen from electrolysis, energy allocation',
                ]
        )
        bioflows_gruped = aggregate_flows(bioflows)
        updated_act.biosphere().delete()
        for flow in bioflows_gruped:
            new_ex = updated_act.new_exchange(input=flow[0], type='biosphere', amount=flow[1])
            new_ex.save()
