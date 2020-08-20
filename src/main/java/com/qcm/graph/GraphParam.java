package com.qcm.graph;

public class GraphParam {
    // name of a person
    public String person;
    // code of the related company
    public String code;
    // if not specify the code, there will be a large number of person with the same name,
    // paging to show them.
    public int start;
    // how many persons(with the same name) are shown in a page?
    public int count;

    public boolean pretty;
}
