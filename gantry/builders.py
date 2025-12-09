from .entities import Patient, Study, Series, Instance, Equipment, DicomItem

class DicomBuilder:
    @staticmethod
    def start_patient(id, name): return PatientBuilder(id, name)

class PatientBuilder:
    def __init__(self, id, name): self.patient = Patient(id, name)
    def add_study(self, uid, date):
        s = Study(uid, date)
        self.patient.studies.append(s)
        return StudyBuilder(self, s)
    def build(self): return self.patient

class StudyBuilder:
    def __init__(self, parent, study): self.parent, self.study = parent, study
    def add_series(self, uid, mod, num):
        s = Series(uid, mod, num)
        self.study.series.append(s)
        return SeriesBuilder(self, s)
    def end_study(self): return self.parent

class SeriesBuilder:
    def __init__(self, parent, series): self.parent, self.series = parent, series
    def set_equipment(self, man, mod, sn=""):
        self.series.equipment = Equipment(man, mod, sn)
        return self
    def add_instance(self, uid, cls, num):
        inst = Instance(uid, cls, num)
        self.series.instances.append(inst)
        return InstanceContextBuilder(self, inst)
    def end_series(self): return self.parent

class InstanceContextBuilder:
    def __init__(self, parent, instance): self.parent, self.instance = parent, instance
    def set_attribute(self, tag, val):
        self.instance.set_attr(tag, val)
        return self
    def set_pixel_data(self, arr):
        self.instance.set_pixel_data(arr)
        return self
    def end_instance(self): return self.parent