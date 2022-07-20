package com.knoma;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.mapper.annotations.DaoFactory;
import com.datastax.oss.driver.api.mapper.annotations.DaoKeyspace;
import com.datastax.oss.driver.api.mapper.annotations.Mapper;
import com.knoma.pojo.PersonDAO;

@Mapper
public interface PersonMapper {

    @DaoFactory
    PersonDAO personDao(@DaoKeyspace CqlIdentifier keyspace);
}