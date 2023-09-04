package com.learningkafka.repository;

import com.learningkafka.entity.LibraryEvent;
import org.springframework.data.repository.CrudRepository;
import org.springframework.stereotype.Repository;

@Repository
public interface LibraryEventRepository  extends CrudRepository<LibraryEvent,Integer> {
}
