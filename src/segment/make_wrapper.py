templates = dict()
templates["[[drop]]"] = "({seg}, {var}) => drop_box!(ptr, {seg}, {var}),\n"
templates["[[new]]"] = "({seg}, {var}) => new!({seg}, {var}),\n"
templates["[[push_univariate]]"] = "({seg}, {var}) => cast!(self, {seg}, {var}).push_univariate(ts, item),\n"
templates["[[push]]"] = "({seg}, {var}) => cast!(self, {seg}, {var}).push(ts, item),\n"
templates["[[to_flush]]"] = "({seg}, {var}) => cast!(self, {seg}, {var}).to_flush(),\n"
templates["[[flushed]]"] = "({seg}, {var}) => cast!(self, {seg}, {var}).flushed(),\n"
templates["[[read]]"] = "({seg}, {var}) => cast!(self, {seg}, {var}).read(),\n"
templates["[[push_item]]"] = "({seg}, {var}) => cast!(self, {seg}, {var}).push_item(ts, item),\n"

segment_max = 10;
variable_max = 50;

def fill(wrapper_template, entry_template, to_replace):

    entries = ""
    for seg in range(1, segment_max + 1):
        for var in range(1, variable_max + 1):
            entries = entries + entry_template.format(seg=seg, var=var)
    return wrapper_template.replace(to_replace, entries)

with open("wrapper_template.rs", "r") as f:
    template = f.read()

for k, v in templates.items():
    template = fill(template, v, k)

with open("wrapper.rs", "w") as f:
    f.write(template)

