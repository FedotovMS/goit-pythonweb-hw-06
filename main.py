from sqlalchemy import select, func, desc
from sqlalchemy.orm import Session

from conf.db import SessionLocal
from entity.models import Student, Grade, Subject, Teacher, Group


# Знаходимо 5 студентів із найбільшим середнім балом з усіх предметів.
def select_01(session: Session):
    query = (
        select(Student.full_name, func.avg(Grade.grade).label("avg_grade"))
        .join(Grade)
        .group_by(Student.id)
        .order_by(desc("avg_grade"))
        .limit(5)
    )
    return session.execute(query).all()


# Знаходимо студента із найвищим середнім балом з певного предмета.
def select_02(session: Session, subject_id: int):
    stmt = (
        select(Student.full_name, func.avg(Grade.grade).label("avg_grade"))
        .join(Grade)
        .where(Grade.subject_id == subject_id)
        .group_by(Student.id)
        .order_by(desc("avg_grade"))
        .limit(1)
    )
    return session.execute(stmt).first()


# Знаходимо середній бал у групах з певного предмета.
def select_03(session: Session, subject_id: int):
    stmt = (
        select(Group.name, func.avg(Grade.grade).label("avg_grade"))
        .select_from(Group)
        .join(Student)
        .join(Grade)
        .where(Grade.subject_id == subject_id)
        .group_by(Group.id)
        .order_by(desc("avg_grade"))
    )
    return session.execute(stmt).all()


# Знаходимо середній бал на потоці (по всій таблиці оцінок).
def select_04(session: Session):
    stmt = select(func.avg(Grade.grade).label("overall_avg_grade"))
    return session.execute(stmt).scalar()


# Знаходимо які курси читає певний викладач.
def select_05(session: Session, teacher_id: int):
    stmt = (
        select(Subject.name)
        .where(Subject.teacher_id == teacher_id)
    )
    return session.execute(stmt).all()


# Знаходимо список студентів у певній групі.
def select_06(session: Session, group_id: int):
    stmt = (
        select(Student.full_name)
        .where(Student.group_id == group_id)
    )
    return session.execute(stmt).all()


# Знаходимо оцінки студентів у окремій групі з певного предмета.
def select_07(session: Session, group_id: int, subject_id: int):
    stmt = (
        select(Student.full_name, Grade.grade, Grade.date_received)
        .join(Grade)
        .where(Grade.subject_id == subject_id, Student.group_id == group_id)
    )
    return session.execute(stmt).all()


# Знаходимо середній бал, який ставить певний викладач зі своїх предметів.
def select_08(session: Session, teacher_id: int):
    stmt = (
        select(func.avg(Grade.grade).label("avg_teacher_grade"))
        .join(Subject)
        .where(Subject.teacher_id == teacher_id)
    )
    return session.execute(stmt).scalar()


# Знаходимо список курсів, які відвідує певний студент.
def select_09(session: Session, student_id: int):
    stmt = (
        select(Subject.name)
        .join(Grade)
        .where(Grade.student_id == student_id)
        .distinct()
    )
    return session.execute(stmt).all()


# Список курсів, які певному студенту читає певний викладач.
def select_10(session: Session, student_id: int, teacher_id: int):
    stmt = (
        select(Subject.name)
        .join(Grade)
        .where(Grade.student_id == student_id, Subject.teacher_id == teacher_id)
        .distinct()
    )
    return session.execute(stmt).all()


if __name__ == "__main__":
    session: Session = SessionLocal()

    # Виведення результатів
    print("1. 5 студентів із найбільшим середнім балом з усіх предметів:")
    students_avg_grades = select_01(session)
    for student in students_avg_grades:
        print(f"  {student[0]}: {student[1]:.2f}")
    
    print("\n2. Студент із найвищим середнім балом з предмета (ID = 2):")
    best_student_subject = select_02(session, 2)
    print(f"  {best_student_subject[0]}: {best_student_subject[1]:.2f}")
    
    print("\n3. Середній бал у групах з предмета (ID = 2):")
    groups_avg_grades = select_03(session, 2)
    for group in groups_avg_grades:
        print(f"  {group[0]}: {group[1]:.2f}")
    
    print("\n4. Середній бал на потоці:")
    overall_avg_grade = select_04(session)
    print(f"  {overall_avg_grade:.2f}")
    
    print("\n5. Курси, які читає викладач (ID = 1):")
    courses_teacher = select_05(session, 1)
    for course in courses_teacher:
        print(f"  {course[0]}")
    
    print("\n6. Список студентів у групі (ID = 3):")
    group_students = select_06(session, 3)
    if group_students:
        for student in group_students:
            print(f"  {student[0]}")
    else:
        print("  У цій групі немає студентів.")
    
    print("\n7. Оцінки студентів у групі (ID = 3) з предмета (ID = 1):")
    student_grades = select_07(session, 3, 1)
    if student_grades:
        for record in student_grades:
            print(f"  {record[0]}: {record[1]} (Date: {record[2]})")
    else:
        print("  Немає оцінок для цієї групи та предмета.")
    
    print("\n8. Середній бал викладача (ID = 1) з його предметів:")
    avg_teacher_grade = select_08(session, 1)
    print(f"  {avg_teacher_grade:.2f}")
    
    print("\n9. Список курсів, які відвідує студент (ID = 1):")
    student_courses = select_09(session, 1)
    for course in student_courses:
        print(f"  {course[0]}")
    
    print("\n10. Курси, які студент (ID = 1) відвідує у викладача (ID = 2):")
    student_teacher_courses = select_10(session, 1, 2)
    for course in student_teacher_courses:
        print(f"  {course[0]}")

    session.close()