# Calliope-ENBIOS integration

## About
This repository provides a workflow to establish a soft coupling between ENBIOS and Calliope, ensuring alignment between 
their implicit and explicit assumptions. Calliope generates cost-optimal system designs for energy scenarios, while 
ENBIOS uses the resulting installed power capacity and energy carrier production across Europe (see Figure below). Additionally, 
ENBIOS incorporates life-cycle inventories for energy infrastructure and energy production 
(including operation and maintenance). This workflow ensures that these inventories are consistent with Calliope's 
assumptions.

![img_2.png](img_2.png)

## Workflow capabilities
### Update the background (sectors other than energy)
Calliope makes a few assumptions on the supply and demand of different economic sectors for 2050. This workflow allows 
adapting the corresponding life-cycle inventories accordingly, if desired:
- **Cement**: clinker production with Carbon Capture and Storage
- **Train**: 100% electrification
- **Biomass**: specified share (biomass_from_residues_share) coming from residues
- **Iron and steel**: from hydrogen - direct reduction iron - 50% electric arc furnace synthetic route
- **Plastics**: olefins produced by methanol. Methanol from H2 and CO2 (from Direct Air Capture)
- **Methanol**: Feedstock methanol from electrolysis (Note: aromatics follow today synthetic route due to lack of data.
            Calliope's assumptions on recycling and improved circular economies could not be matched.)
- **Ammonia**: Feedstock ammonia from hydrogen
- **Transport**: (1) Trucks improved efficiency to EURO6
             (2) Trucks fleet share electrified as specified (trucks_electrification_share)
             (3) Sea transport using synthetic diesel instead of heavy fuel oil
**IMPORTANT**: Note that the changes are only made for Europe, but the rest of the World keeps functioning with the same
production structure as today's.

Summary of LCA background adaptations for each sector:
| **Product/service** | **Description**                                                                                                          | **Inventory name**                                                                                                                  | **Source**                                     |
| ------------------- | ------------------------------------------------------------------------------------------------------------------------ | ----------------------------------------------------------------------------------------------------------------------------------- | ---------------------------------------------- |
| Cement              | Clinker production with on-site CCS                                                                                      | clinker production, efficient, with on-site CCS                                                                                     | Premise additional inventories                 |
| Iron/steel          | Hydrogen direct reduction iron plus electric arc furnace with 50% scrap steel                                            | ‘iron production, from DRI’; ‘steel production, electric, low-alloyed, from DRI-EAF’                                                | New inventory. From various sources            |
| Plastics (olefins)  | Hydrogen-methanol-olefins, which produces propylene, ethylene and butene                                                 | propylene/ethylene/butene production, from methanol (energy allocation)                                                             | From Chen et al. 2024                          |
| Methanol            | Via hydrogen from electrolysis and CO2 from DAC                                                                          | methanol distillation, hydrogen from electrolysis, CO2 from DAC                                                                     | Premise additional inventories                 |
| Ammonia             | Via hydrogen from electrolysis and CO2 from DAC                                                                          | ammonia production, hydrogen from electrolysis                                                                                      | Premise additional inventories                 |
| Kerosene            | No substitution                                                                                                          | No substitution                                                                                                                     | No substitution                                |
| Diesel              | From Fischer Tropsch synthesis via hydrogen from wood gasification                                                       | diesel production, synthetic, from Fischer Tropsch process, hydrogen from wood gasification, energy allocation, at fuelling station | Premise additional inventories                 |
| Biomass             | Biomass as fuel coming partially (share) from forest residues                                                            | market for biomass, used as fuel                                                                                                    | Adaptation from premise additional inventories |
| Sea transport       | Heavy fuel oil substituted by synthetic diesel                                                                           | ‘transport, freight, sea, ...’                                                                                                      | Adaptation from premise additional inventories |
| Road transport      | share (%) ICE– share (%) electric trucks. ICE trucks efficiency updated to EURO6. Diesel substituted by synthetic diesel | ‘transport, freight, lorry, battery electric, ...’                                                                                  | Adaptation from premise additional inventories |

The concept is exemplified with steel in the Figure below.
![img_3.png](img_3.png)

In code, update_background() is the function in charge of these adaptations. The assumptions can be implemented 
 separately by choosing the corresponding booleans in the function run(). Example of accepting all assumptions:
```ruby
run(ccs_clinker=True,
        train_electrification=True,
        biomass_from_residues=True, biomass_from_residues_share=1.0,
        h2_iron_and_steel=True,
        olefins_from_methanol=True,
        methanol_from_electrolysis=True,
        ammonia_from_hydrogen=True,
        trucks_electrification=True, trucks_electrification_share=0.5,
        sea_transport_syn_diesel=True)
```
Example of accepting only the assumption about train electrification, plus the assumption about biomass 
(with 50% of it coming from residues):
```ruby
run(ccs_clinker=False,
        train_electrification=True,
        biomass_from_residues=True, biomass_from_residues_share=0.5,
        h2_iron_and_steel=False,
        olefins_from_methanol=False,
        methanol_from_electrolysis=False,
        ammonia_from_hydrogen=False,
        trucks_electrification=False, trucks_electrification_share=0.5,
        sea_transport_syn_diesel=False)
```

### Adapt the foreground (energy sector inventories)

