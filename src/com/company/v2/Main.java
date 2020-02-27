package com.company.v2;

import btree4j.BTreeException;

import btree4j.Value;
import btree4j.indexer.BasicIndexQuery;
import btree4j.indexer.BasicIndexQuery.IndexConditionBW;
import com.sun.deploy.security.SelectableSecurityManager;
import search.RangeSearcher;

import java.io.*;
import java.util.*;
import java.util.concurrent.ExecutionException;

public class Main {

    public static void main(String[] args)
            throws BTreeException, ExecutionException, InterruptedException, IOException {
        try {
            int colCount = 67140;
            RangeSearcher rangeSearcher = new RangeSearcher("data");
            Scanner input = new Scanner(System.in);
            System.out.println("请输入要进行的操作（1：插入，2：查询）：");
            int oper = input.nextInt();
            switch (oper) {
//                case 1: System.out.println("请输入Gene name数：");
//                int mappingCount = input.nextInt();
//                System.out.println("请输入已有的行数：");
//                int curRow = input.nextInt();
                case 1:
                    int mappingCount = 0;
                    File file = new File("gene_name.tsv");
                    String strLine;
                    BufferedReader bufferedReader = new BufferedReader(new FileReader(file));
                    while (null != (strLine = bufferedReader.readLine())) {
//                System.out.println("第[" + lineCount + "]行数据:[" + strLine + "]")
                        mappingCount++;
                    }
                    int curRow = rangeSearcher.getAllIDs().size();
                    System.out.println("现有行数：" + curRow);
                    System.out.println("Gene name数：" + mappingCount);
                    int mapping[] = new int[mappingCount];
                    String colName[] = new String[colCount];
                    double data[] = new double[colCount];
                    file = new File("ref.txt");
                    bufferedReader = new BufferedReader(new FileReader(file));
                    strLine = null;
                    HashMap<String, Integer> refTable = new HashMap<String, Integer>();
                    long start = System.currentTimeMillis();
                    int lineCount = 1;
                    while (null != (strLine = bufferedReader.readLine())) {
//                System.out.println("第[" + lineCount + "]行数据:[" + strLine + "]");
                        refTable.put(strLine, lineCount - 1); //制作参考表
                        colName[lineCount - 1] = strLine; //制作列名，注意数组下标从0开始
                        if (curRow == 0) rangeSearcher.addColumn(strLine); //如果是空白数据库，则添加column
                        lineCount++;
                    }
                    file = new File("gene_name.tsv");
                    bufferedReader = new BufferedReader(new FileReader(file));
                    lineCount = 1;
                    while (null != (strLine = bufferedReader.readLine())) {
//                System.out.println("第[" + lineCount + "]行数据:[" + strLine + "]");
                        if (refTable.get(strLine) != null)
                            mapping[lineCount - 1] = refTable.get(strLine); //制作映射，这里的第一列对应refTable的第？列
                        else mapping[lineCount - 1] = -1;
                        lineCount++;
                    }
                    refTable = null; //完成历史使命了
                    file = new File("matrix.mtx");
                    bufferedReader = new BufferedReader(new FileReader(file));
                    lineCount = 1;
                    int lastRow = 0;
                    //int cellCount=1;
                    int cellCount = curRow + 1; //是这样的，如果没有细胞，则从第一个开始；若已有n个，则从第n+1个开始。
                    int col = 0, row = 0;
                    double value = 0;
                    while (null != (strLine = bufferedReader.readLine())) {
//                System.out.println("第[" + lineCount + "]行数据:[" + strLine + "]");
                        //strLine为本行数据
                        String[] strArr = strLine.split(" ");
                        row = Integer.valueOf(strArr[0]); //行，列
                        col = Integer.valueOf(strArr[1]);
                        col = mapping[col - 1]; //偏移一下
                        value = Double.valueOf(strArr[2]);
                        if (lineCount != 1 && row != lastRow) {
                            //说明上一个细胞的数据已经输入完毕
                            //插入
                            rangeSearcher.insert(cellCount, colName, data, colCount); //插一行进去
                            if (cellCount % 100 == 0) System.out.println("当前插入第" + cellCount + "个");
                            if (cellCount % 1000 == 0) {
                                rangeSearcher.flush();
                            }
                            cellCount++;
                            Arrays.fill(data, 0); //清零数据，以免出问题
                            if (col != -1) data[col] = value; //新细胞来也
                        } else {
                            if (col != -1) data[col] = value;
                        }
                        lastRow = row;
                        lineCount++;
                    }
                    rangeSearcher.insert(cellCount, colName, data, colCount);
                    rangeSearcher.flush();
                    long end = System.currentTimeMillis();
                    //补插入一次
                    long time = end - start;
                    System.out.println("总共用时" + time + "ms");
                    break;
                case 2:
                    colName = new String[colCount];
//                    file = new File("ref.txt");
//                    bufferedReader = new BufferedReader(new FileReader(file));
//                    lineCount=1;
                    int conCount;
                    double a, b;
//                    while (null != (strLine = bufferedReader.readLine())) {
////                System.out.println("第[" + lineCount + "]行数据:[" + strLine + "]");
//                        colName[lineCount - 1] = strLine; //制作列名，注意数组下标从0开始
//                        lineCount++;
//                    }
                    System.out.println("请输入条件组数：");
                    conCount = input.nextInt();
                    input.nextLine();
                    BasicIndexQuery[] conditions = new BasicIndexQuery[conCount];
                    String[] colNames = new String[conCount];
                    for (int i = 1; i <= conCount; i++) {
                        System.out.println("请输入列名：");
                        colName[i - 1] = input.nextLine();
                        System.out.println("请输入条件（下界 上界）：");
                        a = input.nextDouble();
                        b = input.nextDouble();
                        input.nextLine();
                        conditions[i - 1] = new IndexConditionBW(new Value(a), new Value(b));
                    }
                    start = System.currentTimeMillis();
                    Set<Long> resultSet = rangeSearcher.rangeSearch(colName, conditions, conCount);
                    end = System.currentTimeMillis();
                    time = end - start;
                    System.out.println("总共用时" + time + "ms");
                    break;
                case 3:
                    Set<Long> IDs = rangeSearcher.getAllIDs();
                    System.out.println(IDs.size());
                    break;
                default:
                    System.out.println("无效操作");
                    break;
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
