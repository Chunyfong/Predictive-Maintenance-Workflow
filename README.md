### Predictive-Maintenance-Workflow

Some notes of binary classification on when is the next item replacement or maintenance /Machine failure will occur predication

*Steps*:
1. Generate history of exact date where the repair/customer call for maintenance occurred
2. Backfill 30 days labelled as "1", since we want to "predict" the event happen
3. Gather related data of the event from SQLserver, concate with the label together
4. Set how many day & data of the window we want to use to predict
5. Reshape it to binary classification, array (index,window,param) to predict (index, label)
