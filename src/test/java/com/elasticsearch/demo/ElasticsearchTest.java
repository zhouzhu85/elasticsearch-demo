package com.elasticsearch.demo;

import com.elasticsearch.demo.interfaces.ItemRepository;
import com.elasticsearch.demo.model.Item;
import org.elasticsearch.index.query.MatchQueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.sort.SortBuilders;
import org.elasticsearch.search.sort.SortOrder;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.PageRequest;
import org.springframework.data.domain.Sort;
import org.springframework.data.elasticsearch.core.ElasticsearchTemplate;
import org.springframework.data.elasticsearch.core.query.NativeSearchQueryBuilder;
import org.springframework.test.context.junit4.SpringRunner;

import java.util.ArrayList;
import java.util.List;

@RunWith(SpringRunner.class)
@SpringBootTest(classes = ElasticsearchDemoApplication.class)
public class ElasticsearchTest {
    @Autowired
    private ElasticsearchTemplate elasticsearchTemplate;
    @Autowired
    private ItemRepository itemRepository;
    @Test
    public void testCreate(){
        //创建索引，会根据Item类的@Document注解信息来创建
        elasticsearchTemplate.createIndex(Item.class);
        //配置映射，会根据Item类中的id，Field等字段自动完成映射
        elasticsearchTemplate.putMapping(Item.class);
    }

    /**
     * 添加单条数据
     */
    @Test
    public void index(){
        Item item=new Item(1L,"小米手机8","手机","小米",3499.0,"http://sdfdf.jpg");
        itemRepository.save(item);
    }

    /**
     * 批量添加
     */
    @Test
    public void indexList(){
        List<Item> list=new ArrayList<>();
        //list.add(new Item(2L, "坚果手机R1", " 手机", "锤子", 3699.00, "http://image.leyou.com/123.jpg"));
        //list.add(new Item(3L, "华为META10", " 手机", "华为", 4499.00, "http://image.leyou.com/3.jpg"));
        list.add(new Item(1L, "小米手机7", "手机", "小米", 3299.00, "http://image.leyou.com/13123.jpg"));
        list.add(new Item(2L, "坚果手机R1", "手机", "锤子", 3699.00, "http://image.leyou.com/13123.jpg"));
        list.add(new Item(3L, "华为META10", "手机", "华为", 4499.00, "http://image.leyou.com/13123.jpg"));
        list.add(new Item(4L, "小米Mix2S", "手机", "小米", 4299.00, "http://image.leyou.com/13123.jpg"));
        list.add(new Item(5L, "荣耀V10", "手机", "华为", 2799.00, "http://image.leyou.com/13123.jpg"));
        itemRepository.saveAll(list);
    }

    /**
     * 查询所有
     */
    @Test
    public void findAll(){
        Iterable<Item> items = itemRepository.findAll(Sort.by(Sort.Direction.DESC, "price"));
        items.forEach(item -> System.out.println(item));
    }

    /**
     * 根据自定义字段查询
     */
    @Test
    public void queryByPriceBetween(){
        List<Item> list = itemRepository.findByPriceBetween(2000.00, 3500.00);
        list.forEach(item -> System.out.println("item = "+item));
    }

    /**
     * 基本指定词条查询
     */
    @Test
    public void testQuery(){
        MatchQueryBuilder queryBuilder = QueryBuilders.matchQuery("title", "小米");
        Iterable<Item> items = itemRepository.search(queryBuilder);
        items.forEach(System.out::println);
    }

    /**
     * 自定义查询
     */
    @Test
    public void testNativeQuery(){
        //构建查询条件
        NativeSearchQueryBuilder queryBuilder = new NativeSearchQueryBuilder();
        //添加基本的分词查询
        queryBuilder.withQuery(QueryBuilders.matchQuery("title","小米"));
        //执行搜索，获取结果
        Page<Item> items = itemRepository.search(queryBuilder.build());
        //打印总条数
        System.out.println(items.getTotalElements());
        //打印总页数
        System.out.println(items.getTotalPages());
    }
    /**
     * 自定义分页查询
     */
    @Test
    public void testNativeQueryByPage(){
        //构建查询条件
        NativeSearchQueryBuilder queryBuilder = new NativeSearchQueryBuilder();
        //添加基本的分词查询
        queryBuilder.withQuery(QueryBuilders.matchQuery("category","手机"));
        //初始化分页参数
        int page=0;
        int size=3;
        //设置分页参数
        queryBuilder.withPageable(PageRequest.of(page,size));

        //执行搜索，获取结果
        Page<Item> items = itemRepository.search(queryBuilder.build());
        //打印总条数
        System.out.println("总条数："+items.getTotalElements());
        //打印总页数
        System.out.println("总页数："+items.getTotalPages());
        //每页大小
        System.out.println("每页大小："+items.getSize());
        //当前页
        System.out.println("当前页："+items.getNumber());
        items.forEach(System.out::println);
    }

    /**
     * 排序查询
     */
    @Test
    public void testSort(){
        //构建查询条件
        NativeSearchQueryBuilder queryBuilder=new NativeSearchQueryBuilder();
        //添加基本的分词查询
        queryBuilder.withQuery(QueryBuilders.termQuery("category","手机"));
        //排序
        queryBuilder.withSort(SortBuilders.fieldSort("price").order(SortOrder.DESC));
        //执行搜索，返回结果
        Page<Item> items = itemRepository.search(queryBuilder.build());
        System.out.println("总条数："+items.getTotalElements());
        items.forEach(System.out::println);
    }

}
