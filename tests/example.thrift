namespace cpp space.demo
namespace java space.demo
namespace py space.demo

enum Enum {
  DOWN = 666
  UP = 888
}

typedef i64 Id

struct SimpleObject {
  1: required Id id,
  2: optional i32 status,
  3: Enum action,
  4: bool valid,
  5: set<string> msgs,
  6: map<string, string> mapped,
}

struct ObjectWrapper {
  1: required SimpleObject nested,
  2: list<SimpleObject> nested_list,
  3: map<string, SimpleObject> mapped_obj,
}
