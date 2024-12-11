import bw2data as bd
from tasks import delete_infrastructure_main

bd.projects.set_current('second_test')

delete_infrastructure_main()
